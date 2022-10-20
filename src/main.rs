use anyhow::{ensure, Result};
use clap::Parser;
use futures::stream::{self, StreamExt, TryChunksError, TryStreamExt};
use itertools::Itertools;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    ConnectOptions, FromRow, Pool, QueryBuilder,
};
use std::{collections::HashMap, str::FromStr, thread};

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

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();

    let n_workers = thread::available_parallelism()?.get();

    /* These mnemonics are used. */
    // f: file
    // p: pool
    // t: topic
    // tv: topic vec
    // ti: topic id
    // tn: topic name
    // x_to_y: a map from x to y

    // Open input sqlite databases
    let f_to_p: HashMap<String, Pool<_>> = stream::iter(opts.input_bags)
        .map(|path| async move {
            let uri = format!("sqlite://{}", path);
            let pool = SqlitePoolOptions::new().connect(&uri).await?;
            anyhow::Ok((path, pool))
        })
        .buffer_unordered(n_workers)
        .try_collect()
        .await?;

    // Read topics from input databases
    let f_to_tv: HashMap<&String, Vec<Topic>> = stream::iter(&f_to_p)
        .map(|(path, pool)| async move {
            let topics: Vec<Topic> = sqlx::query_as("SELECT * FROM topics")
                .fetch_all(pool)
                .await?;
            anyhow::Ok((path, topics))
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

    // let f_ti_to_new_ti: HashMap<(String, u32), u32> = f_ti_to_t
    //     .iter()
    //     .map(|((path, orig_topic_id), topic)| {
    //         let topic_name = &topic.name;
    //         let remap_topic_id = tn_to_t[topic_name].id;
    //         ((path.to_string(), *orig_topic_id), remap_topic_id)
    //     })
    //     .collect();

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

    // Gather messages from all input databases
    let message_stream_iter = f_to_p.iter().map(|(path, pool)| {
        sqlx::query_as::<_, Message>("SELECT * FROM messages")
            .fetch(pool)
            .map_ok(move |msg| (path, msg))
    });
    let message_stream = stream::select_all(message_stream_iter);

    // Re-index messages
    let reindexed_message_stream = message_stream
        .enumerate()
        .map(|(msg_id, result)| {
            let (path, msg) = result?;
            anyhow::Ok((msg_id, path, msg))
        })
        .map_ok(|(remap_msg_id, path, msg)| {
            let key = (path, msg.topic_id);
            let topic = &f_ti_to_t[&key];
            let remap_topic_id = tn_to_t[&topic.name].id;
            Message {
                id: remap_msg_id as u32,
                topic_id: remap_topic_id as u32,
                ..msg
            }
        });

    // Insert messages into the output database
    reindexed_message_stream
        .try_chunks(64)
        .map_err(|TryChunksError(_chunk, error)| error)
        .try_fold(output_conn, |mut output_conn, msg_vec| async move {
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
        })
        .await?;

    Ok(())
}
