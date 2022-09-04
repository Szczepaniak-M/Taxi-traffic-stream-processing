#!/bin/bash

BUCKET_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-bucket)

cd ~
hadoop fs -copyToLocal gs://"${BUCKET_NAME}"/taxi_zone_lookup.csv
hadoop fs -copyToLocal gs://"${BUCKET_NAME}"/yellow_tripdata_result.zip
unzip yellow_tripdata_result.zip
wget https://dlcdn.apache.org/flink/flink-1.14.5/flink-1.14.5-bin-scala_2.11.tgz
tar -xzf flink-1.14.5-bin-scala_2.11.tgz

