CLUSTER_NAME=example-cluster-name
BUCKET_NAME=example-bucket-name
PROJECT_ID=example-project-id
REGION=europe-west4
ZONE=${REGION}-c

gcloud beta dataproc clusters create ${CLUSTER_NAME} \
  --enable-component-gateway --bucket ${BUCKET_NAME} \
  --region ${REGION} --subnet default --zone ${ZONE} \
  --master-machine-type n1-standard-4 --master-boot-disk-size 50 \
  --num-workers 2 \
  --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
  --image-version 2.0-debian10 \
  --optional-components ZEPPELIN,ZOOKEEPER,DOCKER \
  --project ${PROJECT_ID} --max-age=3h \
  --metadata "run-on-master=true" \
  --initialization-actions \
  gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh

