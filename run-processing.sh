CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_CLASSPATH=$(hadoop classpath)

~/flink-1.14.5/bin/flink run -m yarn-cluster \
  -p 2 \
  -yjm 1024m \
  -ytm 2048m  \
  ~/TaxiTraffic-1.0.jar \
  --source.static-file ~/taxi_zone_lookup.csv \
  --source.kafka.bootstrap-servers "${CLUSTER_NAME}"-w-0:9092 \
  --destination.kafka.bootstrap-servers "${CLUSTER_NAME}"-w-0:9092 \
  --destination.mysql.url jdbc:mysql://"${CLUSTER_NAME}"-m:3307/taxi_traffic \
  --anomaly-duration 4 \
  --anomaly-size 10000 \
  --history true
