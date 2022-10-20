use anyhow::{ensure, Result};
use async_std::task::spawn;
use clap::Parser;
use futures::{
    future,
    stream::{self, StreamExt, TryStreamExt},
};
use itertools::Itertools;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    ConnectOptions, FromRow, QueryBuilder,
};
use std::{collections::HashMap, str::FromStr, sync::Arc, thread, time::Instant};

/// Rosbag2 merging tool.
#[derive(Parser)]
struct Opts {
    /// Output rosbag2 file.
    #[clap(short, long)]
    pub output_bag: String,

    /// A list of input rosbag2 files to be merged together.
    pub input_bags: Vec<String>,
}

#[derive(Debug, Clone, FromRow)]
struct Topic {
    pub id: u32,
    pub name: String,
    pub r#type: String,
    pub serialization_format: String,
    pub offered_qos_profiles: String,
}

#[derive(Debug, Clone, FromRow)]
struct Message {
    pub id: u32,
    pub topic_id: u32,
    pub timestamp: i64,
    pub data: Vec<u8>,
}

#[async_std::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();

    let n_workers = thread::available_parallelism()?.get();

    /* These mnemonics are used. */
    // f: file
    // c: database connection
    // t: topic
    // tv: topic vec
    // ti: topic id
    // tn: topic name
    // x_to_y: a map from x to y

    // Open input sqlite databases
    let f_to_c: HashMap<&String, _> = stream::iter(&opts.input_bags)
        .map(|path| async move {
            let uri = format!("sqlite://{}", path);
            let pool = SqlitePoolOptions::new().connect(&uri).await?;
            // let pool = SqliteConnectOptions::new().filename(path).connect().await?;
            anyhow::Ok((path, pool))
        })
        .buffer_unordered(n_workers)
        .try_collect()
        .await?;

    // Read topics from input databases
    let f_to_tv: HashMap<&String, Vec<Topic>> = stream::iter(&f_to_c)
        .map(|(path, pool)| async move {
            let topics: Vec<Topic> = sqlx::query_as("SELECT * FROM topics")
                .fetch_all(pool)
                .await?;
            anyhow::Ok((*path, topics))
        })
        .buffer_unordered(n_workers)
        .try_collect()
        .await?;

    // Gather topics from input databases, and
    // build a (path, local_topic_id) -> topic map
    let f_ti_to_t: HashMap<(&String, u32), &Topic> = f_to_tv
        .iter()
        .flat_map(|(path, topic_vec)| topic_vec.iter().map(move |topic| (path, topic)))
        .map(|(&path, topic)| {
            let key = (path, topic.id);
            let value = topic;
            (key, value)
        })
        .collect();

    // Build a (unique_topic_name, topic) map
    let tn_to_t: Vec<(&String, &Topic)> = {
        // Group topics by topic names
        let tn_to_f_t_group: HashMap<&String, Vec<(&String, &Topic)>> = f_ti_to_t
            .iter()
            .map(|((path, _topic_id), topic)| (&topic.name, (*path, *topic)))
            .into_group_map();

        // Check that all topics of the same topic name have identical
        // types, serialization formats and QoS profiles.
        let mut tn_to_t: Vec<(&String, &Topic)> = tn_to_f_t_group
            .into_iter()
            .map(|(topic_name, topic_vec)| {
                let (first_path, first_topic) = &topic_vec[0];

                topic_vec.iter().try_for_each(|(other_path, other_topic)| {
                    ensure!(
                        first_topic.r#type == other_topic.r#type,
                        "topic type differs in {} and {}",
                        first_path,
                        other_path
                    );
                    ensure!(
                        first_topic.serialization_format == other_topic.serialization_format,
                        "serialization format differs in {} and {}",
                        first_path,
                        other_path
                    );
                    ensure!(
                        first_topic.offered_qos_profiles == other_topic.offered_qos_profiles,
                        "QoS profiles differs in {} and {}",
                        first_path,
                        other_path
                    );

                    anyhow::Ok(())
                })?;

                let (_, first_topic) = topic_vec.into_iter().next().unwrap();
                anyhow::Ok((topic_name, first_topic))
            })
            .try_collect()?;

        // Sort topics by topic names
        tn_to_t.sort_by_cached_key(|(topic_name, _)| topic_name.to_string());
        tn_to_t
    };

    // Re-index topics
    let tn_to_t: HashMap<&String, Topic> = tn_to_t
        .into_iter()
        .enumerate()
        .map(|(remap_topic_id, (topic_name, topic))| {
            let remap_topic = Topic {
                id: remap_topic_id as u32,
                ..topic.clone()
            };
            (topic_name, remap_topic)
        })
        .collect();

    let f_to_ti_to_new_ti: HashMap<String, HashMap<u32, u32>> = f_ti_to_t
        .iter()
        .map(|((path, orig_topic_id), topic)| {
            let topic_name = &topic.name;
            let remap_topic_id = tn_to_t[topic_name].id;
            (path.to_string(), (*orig_topic_id, remap_topic_id))
        })
        .into_grouping_map()
        .collect();

    // Gather messages from all input databases
    let f_to_ti_to_new_ti = Arc::new(f_to_ti_to_new_ti);

    // Load messages from input files and re-index topic IDs
    let (loading_futures, msg_stream_vec): (Vec<_>, Vec<_>) = f_to_c
        .into_iter()
        .map(|(path, pool)| {
            let f_to_ti_to_new_ti = f_to_ti_to_new_ti.clone();
            let (tx, rx) = flume::bounded(8);
            let path = path.clone();

            let future = spawn(async move {
                let stream = sqlx::query_as::<_, Message>("SELECT * FROM messages").fetch(&pool);

                let mut stream = stream.map_ok(move |msg| {
                    // let key = (*path, msg.topic_id);
                    let remap_topic_id = f_to_ti_to_new_ti[&path][&msg.topic_id];
                    Message {
                        topic_id: remap_topic_id,
                        ..msg
                    }
                });

                while let Some(msg) = stream.try_next().await? {
                    let ok = tx.send_async(msg).await.is_ok();
                    if !ok {
                        break;
                    }
                }

                anyhow::Ok(())
            });

            (future, rx.into_stream())
        })
        .unzip();

    // Join message streams where messages are ordered by timestamps
    let message_stream =
        join_ordered_streams::join_ordered_streams(|msg| msg.timestamp, msg_stream_vec);

    // Re-index message IDs
    let message_stream = message_stream.enumerate().map(|(msg_id, msg)| Message {
        id: msg_id as u32,
        ..msg
    });

    // Print message rate
    let message_stream = message_stream.scan((Instant::now(), 0), |(instant, count), msg| {
        let secs = instant.elapsed().as_secs_f64();
        *count += 1;

        if secs >= 1.0 {
            eprintln!("{:.3} msgs/s", *count as f64 / secs);
            *instant = Instant::now();
            *count = 0;
        }

        async move { Some(msg) }
    });

    // Connect to output database
    let mut output_conn = {
        let output_url = format!("sqlite://{}", opts.output_bag);
        SqliteConnectOptions::from_str(&output_url)?
            .create_if_missing(true)
            .connect()
            .await?
    };

    // Create tables and indexes in the output database
    {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS topics (\
                 id INTEGER PRIMARY KEY, \
                 name TEXT NOT NULL, \
                 type TEXT NOT NULL, \
                 serialization_format TEXT NOT NULL, \
                 offered_qos_profiles TEXT NOT NULL\
                 )",
        )
        .execute(&mut output_conn)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS messages (\
                 id INTEGER PRIMARY KEY,\
                 topic_id INTEGER NOT NULL,\
                 timestamp INTEGER NOT NULL, \
                 data BLOB NOT NULL\
                 )",
        )
        .execute(&mut output_conn)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS timestamp_idx ON messages (timestamp ASC)")
            .execute(&mut output_conn)
            .await?;
    }

    // Insert topics to the output database
    QueryBuilder::new(
        "INSERT INTO topics (\
         id, name, type, serialization_format, offered_qos_profiles\
         ) ",
    )
    .push_values(tn_to_t.values(), |mut batch, topic| {
        batch.push_bind(topic.id);
        batch.push_bind(&topic.name);
        batch.push_bind(&topic.r#type);
        batch.push_bind(&topic.serialization_format);
        batch.push_bind(&topic.offered_qos_profiles);
    })
    .build()
    .execute(&mut output_conn)
    .await?;

    // Insert messages into the output database
    let write_future = message_stream.chunks(8).map(Ok).try_fold(
        output_conn,
        |mut output_conn, msg_vec| async move {
            QueryBuilder::new(
                "INSERT INTO messages (\
                 id, topic_id, timestamp, data\
                 ) ",
            )
            .push_values(msg_vec, |mut batch, msg: Message| {
                batch.push_bind(msg.id);
                batch.push_bind(msg.topic_id);
                batch.push_bind(msg.timestamp);
                batch.push_bind(msg.data);
            })
            .build()
            .execute(&mut output_conn)
            .await?;

            anyhow::Ok(output_conn)
        },
    );

    future::try_join(write_future, future::try_join_all(loading_futures)).await?;

    Ok(())
}
