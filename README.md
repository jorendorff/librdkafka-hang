
This program demonstrates a kafka consumer freeze when using librdkafka's `BaseConsumer`.
Follow these steps to reproduce the behavior:

## Setup

-   Bring up docker-compose environment
  
    ```sh
    docker-compose up -d
    ```

-   Publish some messages to kafka

    ```sh
    ./publish_messages.sh
    ```

## Run with `--store-offsets` turned on (this freezes)

```rust
cargo run -- --store-offsets
```

The program does not print "`leaving main`", and does not exit.

(You can use Ctrl+C to kill it.)


## Run with `--store-offsets` turned off (this one does not freeze)

```rust
cargo run
```

The program prints "`leaving main`" and exits after 3 seconds.
