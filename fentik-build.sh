#!/bin/bash

# To clean EVERYTHING:
# git clean -fdx

mvn install -DskipTests -Dfast
cp ./flink-connectors/flink-sql-connector-kafka/target/flink-sql-connector-kafka-1.15.0.jar /opt/flink-1.15.0-rc1/lib/
