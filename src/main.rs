use anyhow::{Context, Result};
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{BaseConsumer, Consumer},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use std::{
    env,
    time::{Instant, Duration},
};

fn main() {
    let args = env::args().skip(1).collect::<Vec<String>>();
    let store_offsets = args == vec!["--store-offsets"];

    let mut consumer =
        PartitionConsumer::new(store_offsets).expect("partition consumer");

    while let Some(msg) = consumer.next() {
        eprintln!("message received: {:?}", msg);
    }

    eprintln!("leaving main");
}

pub struct PartitionConsumer {
    consumer: BaseConsumer,
    topic_partition: TopicPartitionList,
    start_time: Instant,
    store_offsets: bool,
}

impl PartitionConsumer {
    fn new(store_offsets: bool) -> Result<Self> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("group.id", "test")
            .set("bootstrap.servers", "localhost:29094")
            .set("enable.partition.eof", "false")
            .set("enable.auto.offset.store", "false") // We will control the consumer's in-memory offset store
            .set("enable.auto.commit", "true") // same as default value: let librdkafka handle committing offsets
            .set("debug", "consumer,cgrp,topic,fetch") // enable very verbose, low-level kafka logging
            .set_log_level(RDKafkaLogLevel::Debug);

        let topic = "test";
        let partition = 0;
        let mut topic_partition = TopicPartitionList::new();
        topic_partition
            .add_partition_offset(topic, partition, Offset::Beginning)
            .with_context(|| format!("failed to set partition {} offset", partition))?;

        let consumer: BaseConsumer = client_config
            .create()
            .with_context(|| "kafka consumer creation failed")?;
        consumer
            .assign(&topic_partition)
            .with_context(|| "failed to assign to topic partition list")?;

        Ok(Self {
            consumer,
            topic_partition,
            start_time: Instant::now(),
            store_offsets,
        })
    }

    fn store_offset(&mut self) {
        if self.store_offsets {
            self.topic_partition
                .set_all_offsets(Offset::Offset(0))
                .expect("error updating partition list");
            self.consumer.store_offsets(&self.topic_partition)
                .expect("error storing offsets");
        }
    }

    fn next(&mut self) -> Option<String> {
        while self.start_time.elapsed() < Duration::from_secs(3) {
            self.store_offset();

            match self.consumer.poll(Duration::from_secs(1)) {
                Some(Ok(msg)) => {
                    let payload = msg.payload().expect("payload");
                    let content = std::str::from_utf8(payload)
                        .expect("string content")
                        .to_string();
                    return Some(content);
                }
                Some(Err(e)) =>
                    eprintln!("error polling for kafka message: {}", e),
                None => {}
            }
        }
        eprintln!("3 seconds passed, exiting");
        None
    }
}
