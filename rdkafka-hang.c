#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "librdkafka/rdkafka.h"

void config(rd_kafka_conf_t *conf, const char *key, const char *value) {
  char err_buf[200];
  rd_kafka_conf_res_t ret = rd_kafka_conf_set(conf, key, value, err_buf, sizeof err_buf);
  if (ret != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "error configuring kafka client: %s\n", err_buf);
    exit(1);
  }
}

static int store_offsets;

// Note: Copied from the documentation for `rd_kafka_conf_set_rebalance_cb`.
static void rebalance_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err,
                         rd_kafka_topic_partition_list_t *partitions,
                         void *opaque) {
  switch (err) {
  case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
    // application may load offets from arbitrary external
    // storage here and update \p partitions
    if (!strcmp(rd_kafka_rebalance_protocol(rk), "COOPERATIVE"))
      rd_kafka_incremental_assign(rk, partitions);
    else // EAGER
      rd_kafka_assign(rk, partitions);
    break;

  case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
    if (store_offsets) // Optional explicit manual commit
      rd_kafka_commit(rk, partitions, 0); // sync commit

    if (!strcmp(rd_kafka_rebalance_protocol(rk), "COOPERATIVE"))
      rd_kafka_incremental_unassign(rk, partitions);
    else // EAGER
      rd_kafka_assign(rk, NULL);
    break;

  default:
    fprintf(stderr, "rebalance_cb: unrecogized error: %s\n", rd_kafka_err2str(err));
    rd_kafka_assign(rk, NULL); // sync state
    break;
  }
}

void offset_commit_cb(rd_kafka_t *rk,
                      rd_kafka_resp_err_t err,
                      rd_kafka_topic_partition_list_t *offsets,
                      void *opaque) {
  fprintf(stderr, "\n*** commit: %s\n", rd_kafka_err2str(err));
  for (int i = 0; i < offsets->cnt; i++) {
    rd_kafka_topic_partition_t *tp = &offsets->elems[i];
    fprintf(stderr, "  topic '%s' partition %d: offset %ld\n", tp->topic, (int) tp->partition, (long int) tp->offset);
  }
  fprintf(stderr, "\n");
}

int main(int argc, const char *argv[]) {
  store_offsets = (argc == 2 && strcmp(argv[1], "--store-offsets") == 0);

  rd_kafka_topic_partition_list_t *tpl = rd_kafka_topic_partition_list_new(1);
  if (!tpl) {
    fprintf(stderr, "rd_kafka_topic_partition_list_new failed\n");
    exit(1);
  }

  rd_kafka_topic_partition_t *tp_ptr = rd_kafka_topic_partition_list_add(tpl, "test", 0);
  rd_kafka_resp_err_t err = rd_kafka_topic_partition_list_set_offset(tpl, "test", 0, RD_KAFKA_OFFSET_BEGINNING);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    fprintf(stderr, "rd_kafka_topic_partition_list_set_offset failed: %s", rd_kafka_err2str(err));
    exit(1);
  }

  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  if (!conf) {
    fprintf(stderr, "rd_kafka_conf_new failed\n");
    exit(1);
  }
  config(conf, "group.id", "test");
  config(conf, "bootstrap.servers", "localhost:29094");
  config(conf, "enable.partition.eof", "false");
  config(conf, "enable.auto.offset.store", "false");
  config(conf, "enable.auto.commit", "true");
  config(conf, "debug", "consumer,cgrp,topic,fetch,broker");

  rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);
  rd_kafka_conf_set_offset_commit_cb(conf, offset_commit_cb);

  char err_buf[200];
  rd_kafka_conf_set_opaque(conf, NULL);
  rd_kafka_t *client = rd_kafka_new(RD_KAFKA_CONSUMER, conf, err_buf, sizeof err_buf);
  if (!client) {
    fprintf(stderr, "rd_kafka_new failed: %s\n", err_buf);
    exit(1);
  }

  err = rd_kafka_assign(client, tpl);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    fprintf(stderr, "rd_kafka_assign failed: %s\n", rd_kafka_err2str(err));
    exit(1);
  }

  for (;;) {
    rd_kafka_poll(client, 0);
    rd_kafka_message_t *message = rd_kafka_consumer_poll(client, 1000);
    if (!message) {
      break;
    } else if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      fprintf(stderr, "rd_kafka_consumer_poll failed: %s\n", rd_kafka_err2str(message->err));
      exit(1);
    }
    fprintf(stderr, "message received: \"");
    fwrite(message->payload, 1, message->len, stderr);
    fprintf(stderr, "\"\n");

    if (store_offsets) {
      tpl->elems[0].offset = message->offset + 1;
      err = rd_kafka_offsets_store(client, tpl);
      if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "rd_kafka_offsets_store failed: %s\n", rd_kafka_err2str(err));
        exit(1);
      }
    }

    rd_kafka_message_destroy(message);
  }

  rd_kafka_consumer_close(client);
  fprintf(stderr, "Destroying client\n");
  rd_kafka_destroy(client);
  fprintf(stderr, "Success\n");
  rd_kafka_topic_partition_list_destroy(tpl);
  return 0;
}
