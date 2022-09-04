#!/bin/bash

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

/usr/lib/kafka/bin/kafka-topics.sh \
  --create \
  --zookeeper "${CLUSTER_NAME}"-m:2181 \
  --replication-factor 1 \
  --partitions 2 \
  --topic taxi-traffic-anomalies
