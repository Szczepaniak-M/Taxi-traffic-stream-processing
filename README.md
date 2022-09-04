# Taxi Traffic Stream Processing

A project uses Flink and Scala to process data available in Kafka topic about taxi traffic.
Processed data are then saved to MySQL database and to another Kafka topic.

## Data
The source of the data is `https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page`
The data are available in `taxi-traffic-data.zip`. The archive contains 101 files:
- 100 CSV files in directory `trip-data-record` with the events about starts and ends of the taxi rides,
- `taxi_zone_lookup.csv` with the data about taxi zones and borough in New York

## Data processing 
### Loading data to Kafka topic
The data in file in the `taxi-data-record` directory are read line by line by `TaxiEventProducerMain` and send to Kafka topic.
Afterwards, these data are read and processed by the main program of the project.

### Stream processing
The most important class of the project is `TaxiTrafficStreamMain` which is responsible for stream processing.
Firstly, data are read from Kafka topic using a proper Flink connector.
Then, the data are processed in two ways:
- aggregating number of arrival and departure and the number of passengers by date and borough
- discovering anomalies - more than allowed number of people have arrived to or leaved 
any borough in the given period (the values are parametrized via execution parameters `anomaly-size` and `anomaly-duration`)

### Storing aggregated data
Aggregated data are stored in MySQL database using a proper connector in to the table with the following schema:
```sql
CREATE TABLE taxi_traffic
(
    date                      DATE        NOT NULL,
    borough                   VARCHAR(31) NOT NULL,
    arrival_count             INT         NOT NULL,
    departure_count           INT         NOT NULL,
    arrival_passenger_count   INT         NOT NULL,
    departure_passenger_count INT         NOT NULL,
    PRIMARY KEY (date, borough)
);
```
The save to database are triggered every 1 hour or 10 seconds of the event time 
(depends on execution parameter `history`)

The data from the MySQL table can be read using `AggregationConsumerMain` program, 
which prints the content of the table (last 10 rows) to the console every minute.

### Storing anomalies
Discovered anomalies are send to Kafka topic.
Each message contains information about:
- borough, 
- start datetime of anomaly
- end datetime of anomaly
- number of passengers who arrived
- number of passengers who departure
- difference in the number of passengers

The data from the Kafka topic can be read using `AnomalyConsumerMain` program,
which poll Kafka topic every 15 seconds.

## Technologies
The following technologies and libraries are used by the project:
- Scala 2.11
- Flink 1.14
- Kafka
- MySQL

## Running
- `setup.sh` - the script which create Kafka topics, set up MySQL database in Docker container,
download files from the buckets and build the JAR
- `run-producer.sh` - the script which run `TaxiEventProducerMain` with proper parameters
- `run-processing.sh` - the script which run `TaxiEventProducerMain` with proper parameters
- `run-aggregation-consumer.sh` - the script which run `AggregationConsumerMain` with proper parameters
- `run-anomaly-consumer` - the script which run `AnomalyConsumerMain` with proper parameters