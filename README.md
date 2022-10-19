# Rosbag2 Merging Tool 

This project provides the `rosbag2-merge` tool that merges multiple
rosbag2 files into one file.

## Build

Rust toolchain is required to build this project. If you haven't
install it yet, visit https://rustup.rs/ and follow the instructions.


Compile this project by

```bash
cargo build --release
```

## Usage

This command merges `input1.db3`, `input2.db3` and `input3.db3`
rosbag2 files into a `output.db3` file.

```bash
./target/release/rosbag2-merge -o output.db3 input1.db3 input2.db3 input3.db3
```

## License

MIT license. Please check the license text in
[LICENSE.txt](LICENSE.txt).
