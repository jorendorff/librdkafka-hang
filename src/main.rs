use anyhow::{Context, Result};
use futures::StreamExt;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{BaseConsumer, Consumer},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use signal_hook::consts::TERM_SIGNALS;
use signal_hook_tokio::{Signals, SignalsInfo};
use std::{
    env,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};
use tokio::{select, sync::oneshot};

#[tokio::main]
async fn main() {
    let args = env::args().collect::<Vec<String>>();
    let mut store_offsets = false;
    if args.len() > 1 {
        store_offsets = &args[1] == "--store-offsets"
    }

    let client_config = client_config().unwrap();
    let stop_consumer = Arc::new(AtomicBool::new(false));
    let consumer = PartitionConsumer::new(
        &client_config,
        "test",
        0,
        stop_consumer.clone(),
        store_offsets,
    )
    .expect("partition consumer");
    let signals = Signals::new(TERM_SIGNALS).expect("failed to install signal handler");
    let handle = signals.handle();

    let shutdown = shutdown(signals);

    println!("consuming from kafka");
    let (idx_thread_sender, mut idx_thread_recv) = oneshot::channel::<()>();

    let consume_thread = thread::spawn(move || {
        let _idx_thread_sender = idx_thread_sender;
        consumer.for_each(|msg| println!("msg : {}", msg));
    });

    tokio::pin!(shutdown);

    shutdown.await;
    eprintln!("shutdown requested, killing kafka thread...");
    stop_consumer.store(true, Ordering::SeqCst);

    consume_thread.join().expect("consumer thread to finish");
    handle.close();
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
    fn new(
        client_config: &ClientConfig,
        topic: &str,
        partition: i32,
        stop_consumer: Arc<AtomicBool>,
        store_offsets: bool,
    ) -> Result<Self> {
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

    fn store_offset(&mut self) -> Result<()> {
        if self.store_offsets {
            self.topic_partition
                .set_all_offsets(Offset::Offset(self.offset))?;
            self.consumer.store_offsets(&self.topic_partition)?;
        }
        Ok(())
    }
}

impl Iterator for PartitionConsumer {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.stop_consumer.load(Ordering::SeqCst) {
            self.store_offset()
                .unwrap_or_else(|e| eprintln!("error storing kafka offset: {}", e));

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
                        e,
                        self.partition
                    );
                }
                None => {}
            }
        }
        None
    }
}

/// Return configuration for creating a Kafka consumer or metadata client.
/// See the [librdkafka documentation](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html) for options and defaults.
pub fn client_config() -> Result<ClientConfig> {
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
        // NB: This will enable very verbose, low-level kafka logging
        .set("debug", "consumer,cgrp,topic,fetch")
        .set_log_level(RDKafkaLogLevel::Debug);

    Ok(client_config)
}

async fn shutdown(signals: SignalsInfo) {
    let mut signals = signals.fuse();
    signals.next().await;
    eprintln!("shutdown function finished");
}
