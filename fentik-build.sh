#!/bin/bash

# To clean EVERYTHING:
# mvn clean
# git clean -fdx

set -ex

FLINK_DIR=build-target
LIB_DIR="$FLINK_DIR/lib/"
OPT_DIR="$FLINK_DIR/opt/"
GIT_SHA=$(git log -n 1 --format="%H" .)

#mvn install -DskipTests -Dfast -f fentik-udf
#mvn install -DskipTests -Dfast

# Copy some required libraries that are not part of the core Flink distribution.
#cp ./flink-connectors/flink-sql-connector-kafka/target/flink-sql-connector-kafka-1.15.0.jar $LIB_DIR
#cp ./flink-table/flink-table-runtime/target/flink-table-runtime-1.15.0.jar  $LIB_DIR
#cp ./flink-metrics/flink-metrics-prometheus/target/flink-metrics-prometheus-1.15.0.jar $LIB_DIR
#cp ./flink-connectors/flink-connector-hive/target/flink-connector-hive_2.12-1.15.0.jar $OPT_DIR

aws s3 cp s3://dev-dataflo/ops/ec2/flink-lib/libfb303-0.9.3.jar $LIB_DIR
aws s3 cp s3://dev-dataflo/ops/ec2/flink-lib/hive-exec-3.1.2.jar $LIB_DIR
aws s3 cp s3://dev-dataflo/ops/ec2/fentik-rescale-savepoint-0.1.0.jar $OPT_DIR

mkdir -p $FLINK_DIR/plugins/s3-fs-presto
#cp ./flink-filesystems/flink-s3-fs-presto/target/flink-s3-fs-presto-1.15.0.jar $FLINK_DIR/plugins/s3-fs-presto/

if [ "$1" == "--package" ]; then
    # Build a Flink binary.
    echo "Building Flink tarball"
    rm -f /tmp/flink.tar.gz
    pushd $FLINK_DIR
    tar --exclude conf/flink-conf.yaml -zchf /tmp/flink.tar.gz .
    popd
    S3_PATH="s3://prod-dataflo/ops/ec2/flink/$GIT_SHA/flink.tar.gz"
    S3_PATH_LATEST="s3://prod-dataflo/ops/ec2/flink/latest/flink.tar.gz"
    aws s3 cp /tmp/flink.tar.gz $S3_PATH
    aws s3 cp /tmp/flink.tar.gz $S3_PATH_LATEST
    echo "To use the new binary, update python/scripts/setup_ec2/common.sh with":
    echo "FLINK_BINARY_PATH=$S3_PATH"
fi
