# Distributed key-value store

## Installation and usage

You need [Rust](https://www.rust-lang.org/tools/install) 2021 edition or later.
Run

```sh
cargo build --release
```

to build the project and then start a node by running

```sh
DS_KNOWN_NODE=123.123.123.123 cargo run --release
```

Replace `123.123.123.123` with the IP address of a single known node in the datastore system.
Alternatively do not supply the environment variable at all to start a new datastore.

### Docker

This project also supports Docker.
Instead of building and running the application directly, you can build a Docker image by running

```sh
docker build -t ds-project .
```

Then you can start a Docker container with

```sh
docker run -p 52525:52525 ds-project
```

### Client

Although the client using the key-value store is out from the scope of this project,
a simple client written in Python (version 3.11 or later) is included for demonstration purposes.
For more information, run

```sh
python client/main.py --help
```
