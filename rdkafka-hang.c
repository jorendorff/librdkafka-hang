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

int main(int argc, const char *argv[]) {
  int store_offsets = (argc == 2 && strcmp(argv[1], "--store-offsets") == 0);

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
  config(conf, "debug", "consumer,cgrp,topic,fetch");

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
      tpl->elems[0].offset = message->offset;
      err= rd_kafka_offsets_store(client, tpl);
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
