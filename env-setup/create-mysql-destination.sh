#!/bin/bash

docker run \
  -p 3307:3306 \
  -e MYSQL_ROOT_PASSWORD=password \
  -e MYSQL_DATABASE=taxi_traffic \
  -e MYSQL_ADMIN=admin \
  -e MYSQL_PASSWORD=password \
  -v "$(pwd)"/setup.sql:/docker-entrypoint-initdb.d/setup.sql:ro \
  -d mysql:latest
