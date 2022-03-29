use anyhow::{Context, Result};
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{BaseConsumer, Consumer},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use std::{
    env,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

fn main() {
    let args = env::args().collect::<Vec<String>>();
    let mut store_offsets = false;
    if args.len() > 1 {
        store_offsets = &args[1] == "--store-offsets"
    }

    let stop_consumer = Arc::new(AtomicBool::new(false));
    let mut consumer =
        PartitionConsumer::new(stop_consumer.clone(), store_offsets).expect("partition consumer");

    eprintln!("starting consumer thread...");

    let consume_thread = thread::spawn(move || {
        eprintln!("consumer thread started");
        while let Some(msg) = consumer.next() {
            eprintln!("message received: {:?}", msg);
        }
    });

    eprintln!("sleeping 3 seconds...");
    thread::sleep(Duration::from_secs(3));
    eprintln!("sleep done, shutting down kafka thread...");
    stop_consumer.store(true, Ordering::SeqCst);

    consume_thread.join().expect("consumer thread to finish");
    eprintln!("leaving main");
}

pub struct PartitionConsumer {
    partition: i32,
    consumer: BaseConsumer,
    topic_partition: TopicPartitionList,
    stop_consumer: Arc<AtomicBool>,
    offset: i64,
    store_offsets: bool,
}

impl PartitionConsumer {
    fn new(stop_consumer: Arc<AtomicBool>, store_offsets: bool) -> Result<Self> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("group.id", "test")
            .set("bootstrap.servers", "localhost:29094")
            .set("enable.partition.eof", "false")
            .set("max.poll.interval.ms", "300000") // same as default value
            .set("heartbeat.interval.ms", "3000") // same as default value
            .set("session.timeout.ms", "45000") // same as default value
            .set("enable.auto.offset.store", "false") // We will control the consumer's in-memory offset store
            .set("enable.auto.commit", "true") // same as default value: let librdkafka handle committing offsets
            .set("debug", "consumer,cgrp,topic,fetch") // enable very verbose, low-level kafka logging
            .set_log_level(RDKafkaLogLevel::Debug);

        let topic = "test";
        let partition = 0;
        let offset = Offset::Beginning;
        let mut topic_partition = TopicPartitionList::new();
        topic_partition
            .add_partition_offset(topic, partition, offset)
            .with_context(|| format!("failed to set partition {} offset", partition))?;

        let consumer: BaseConsumer = client_config
            .create()
            .with_context(|| "kafka consumer creation failed")?;
        consumer
            .assign(&topic_partition)
            .with_context(|| "failed to assign to topic partition list")?;

        Ok(Self {
            consumer,
            partition,
            topic_partition,
            stop_consumer,
            offset: 0,
            store_offsets,
        })
    }

    fn store_offset(&mut self) {
        if self.store_offsets {
            self.topic_partition
                .set_all_offsets(Offset::Offset(self.offset))
                .expect("error updating partition list");
            self.consumer.store_offsets(&self.topic_partition)
                .expect("error storing offsets");
        }
    }

    fn next(&mut self) -> Option<String> {
        while !self.stop_consumer.load(Ordering::SeqCst) {
            self.store_offset();

            match self.consumer.poll(Duration::from_secs(1)) {
                Some(Ok(msg)) => {
                    let payload = msg.payload().expect("payload");
                    let content = std::str::from_utf8(payload)
                        .expect("string content")
                        .to_string();
                    return Some(content);
                }
                Some(Err(e)) => {
                    eprintln!(
                        "error polling for kafka message: {}, partition:{}",
                        e, self.partition
                    );
                }
                None => {}
            }
        }
        eprintln!("stop requested, exiting consumer thread");
        None
    }
}
