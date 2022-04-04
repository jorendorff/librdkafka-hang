use std::env;
use std::time::{Duration, Instant};

use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

fn main() {
    let args = env::args().skip(1).collect::<Vec<String>>();
    let store_offsets = args == vec!["--store-offsets"];
    run_test(store_offsets);
    eprintln!("leaving main");
}

fn run_test(store_offsets: bool) {
    let mut client_config = ClientConfig::new();
    client_config
        .set("group.id", "test")
        .set("bootstrap.servers", "localhost:29094")
        .set("enable.partition.eof", "false")
        .set("enable.auto.offset.store", "false") // application will set consumer offsets explicitly
        .set("enable.auto.commit", "true") // same as default value: let librdkafka handle committing offsets
        .set("debug", "consumer,cgrp,topic,fetch") // enable low-level rdkafka logging
        .set_log_level(RDKafkaLogLevel::Debug);

    let mut topic_partition = TopicPartitionList::new();
    topic_partition
        .add_partition_offset("test", 0, Offset::Beginning)
        .unwrap();

    let consumer: BaseConsumer = client_config.create().unwrap();
    consumer.assign(&topic_partition).unwrap();

    let start_time = Instant::now();
    while start_time.elapsed() < Duration::from_secs(3) {
        match consumer.poll(Duration::from_secs(1)) {
            Some(Ok(msg)) => {
                let payload = msg.payload().unwrap();
                let content = std::str::from_utf8(payload).unwrap();
                eprintln!("message received: {:?}", content);
                if store_offsets {
                    let offset = Offset::Offset(msg.offset() + 1);
                    topic_partition.set_all_offsets(offset).unwrap();
                    consumer.store_offsets(&topic_partition).unwrap();
                }
            }
            Some(Err(e)) => eprintln!("error polling for kafka message: {}", e),
            None => {}
        }
    }
    eprintln!("3 seconds passed, exiting");
}
