CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

java -cp ~/TaxiTraffic-1.0.jar pl.michalsz.bigdata.TaxiEventProducerMain \
  ~/yellow_tripdata_result \
  taxi-events \
  15 \
  "${CLUSTER_NAME}"-w-0:9092
