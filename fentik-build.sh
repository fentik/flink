#!/bin/bash

# To clean EVERYTHING:
# git clean -fdx

mvn install -DskipTests -Dfast

# Copy some required libraries that are not part of the core Flink distribution.
LIB_DIR="/opt/flink-1.15.0-rc1/lib/"
cp ./flink-connectors/flink-sql-connector-kafka/target/flink-sql-connector-kafka-1.15.0.jar $LIB_DIR
cp ./flink-connectors/flink-connector-hive/target/flink-connector-hive_2.12-1.15.0.jar $LIB_DIR
aws s3 cp s3://dev-dataflo/ops/ec2/flink-lib/libfb303-0.9.3.jar $LIB_DIR
aws s3 cp s3://dev-dataflo/ops/ec2/flink-lib/hive-exec-3.1.2.jar $LIB_DIR
