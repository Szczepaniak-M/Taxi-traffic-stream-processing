#!/bin/bash

cd env-setup
sh ./download-files.sh
sh ./create-kafka-source.sh
sh ./create-kafka-destination.sh
sh ./create-mysql-destination.sh
sudo apt-get -y update
sudo apt-get -y install maven
cd ~/ProjectFlink
mvn clean install
cp target/TaxiTraffic-1.0-jar-with-dependencies.jar ~/TaxiTraffic-1.0.jar






