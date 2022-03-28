
This program demonstrates a kafka consumer freeze when using librdkafka's `BaseConsumer`. The wrapping `PartitionConsumer` manages the offsets and not the `BaseConsumer`. Follow these steps to reproduce the behavior:

## Setup

- Bring up docker-compose environment
  
```sh
docker-compose up -d
```

- Publish some messages to kafka

```sh
./publish_messages.sh
```

## Run with `--store-offsets` turned on (this freezes)

```rust
cargo run -- --store-offsets
```

Try to exit the program with `ctr+c` (you can't exit)


## Run with `--store-offsets` turned off (this one does not freeze)

```rust
cargo run -- --store-offsets
```

Try to exit the program with `ctr+c` (this exits)
