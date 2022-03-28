#!/bin/bash

set -e

docker-compose exec -T kafka kafka-topics --create --bootstrap-server localhost:29094 --replication-factor 1 --topic test

for i in {1..5}
do
  echo "msg-$i" | docker-compose exec -T kafka kafka-console-producer --topic test --bootstrap-server localhost:29094
done

