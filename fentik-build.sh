#!/bin/bash

# To clean EVERYTHING:
# mvn clean
# git clean -fdx

set -ex

FLINK_DIR=build-target
FLINK_BASE=flink-1.15.0-rc1
LIB_DIR="$FLINK_DIR/lib/"
OPT_DIR="$FLINK_DIR/opt/"
GIT_SHA=$(git log -n 1 --format="%H" .)

mvn install -DskipTests -Dfast -f fentik-udf
mvn install -DskipTests -Dfast

# Copy some required libraries that are not part of the core Flink distribution.
cp ./flink-connectors/flink-sql-connector-kafka/target/flink-sql-connector-kafka-1.15.0.jar $LIB_DIR
cp ./flink-table/flink-table-runtime/target/flink-table-runtime-1.15.0.jar  $LIB_DIR
cp ./flink-metrics/flink-metrics-prometheus/target/flink-metrics-prometheus-1.15.0.jar $LIB_DIR
cp ./flink-connectors/flink-connector-hive/target/flink-connector-hive_2.12-1.15.0.jar $OPT_DIR

aws s3 cp s3://dev-dataflo/ops/ec2/flink-lib/libfb303-0.9.3.jar $LIB_DIR
aws s3 cp s3://dev-dataflo/ops/ec2/flink-lib/hive-exec-3.1.2.jar $LIB_DIR
aws s3 cp s3://dev-dataflo/ops/ec2/fentik-rescale-savepoint-0.1.0.jar $OPT_DIR

mkdir -p $FLINK_DIR/plugins/s3-fs-presto
cp ./flink-filesystems/flink-s3-fs-presto/target/flink-s3-fs-presto-1.15.0.jar $FLINK_DIR/plugins/s3-fs-presto/

if [ "$1" == "--package" ]; then
    # Build a Flink binary.
    temp_dir=$(mktemp -d)
    echo "Building Flink tarball"
    # Note: we want the tarball to start with ./$FLINK_BASE/.
    mkdir $temp_dir/target
    ln -s $PWD/$FLINK_DIR $temp_dir/target/$FLINK_BASE
    pushd $temp_dir/target
    tar --exclude conf/flink-conf.yaml -zchf ../flink.tar.gz .
    popd
    S3_PATH="s3://prod-dataflo/ops/ec2/flink/$GIT_SHA/flink.tar.gz"
    S3_PATH_LATEST="s3://prod-dataflo/ops/ec2/flink/latest/flink.tar.gz"
    aws s3 cp $temp_dir/flink.tar.gz $S3_PATH
    aws s3 cp $temp_dir/flink.tar.gz $S3_PATH_LATEST
    rm -rf $temp_dir
    echo "To use the new binary, update python/scripts/setup_ec2/common.sh with":
    echo "FLINK_BINARY_PATH=$S3_PATH"
fi
