
This program demonstrates a kafka consumer freeze when using librdkafka's `BaseConsumer`.
Follow these steps to reproduce the behavior:

## Setup

1.  Bring up docker-compose environment
  
    ```sh
    docker-compose up -d
    ```

2.  Publish some messages to kafka

    ```sh
    ./publish_messages.sh
    ```

3.  Run one of the test programs below to observe the issue.


## Running the C program

```sh
make
./rdkafka-hang                   # exits normally, reporting "Success"
./rdkafka-hang --store-offsets   # never exits
```

## Running the Rust program

```sh
cargo run
```

The program prints "`leaving main`" and exits after 3 seconds.

```sh
cargo run -- --store-offsets
```

The program does not print "`leaving main`", and does not exit.

(You can use Ctrl+C to kill it.)

