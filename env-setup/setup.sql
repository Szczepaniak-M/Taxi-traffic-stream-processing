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

CREATE USER 'flink_user'@'%' IDENTIFIED BY 'password';

GRANT ALL ON taxi_traffic.* TO 'flink_user'@'%';
