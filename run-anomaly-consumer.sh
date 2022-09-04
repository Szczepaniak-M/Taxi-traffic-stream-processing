CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

java -cp ~/TaxiTraffic-1.0.jar pl.michalsz.bigdata.AnomalyConsumerMain "${CLUSTER_NAME}"-w-0:9092
