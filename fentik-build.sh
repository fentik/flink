#!/bin/bash

# To clean EVERYTHING:
# git clean -fdx

FLINK_BASE=flink-1.15.0-rc1

pushd fentik-udf
mvn install -DskipTests -Dfast
popd

mvn install -DskipTests -Dfast

# Copy some required libraries that are not part of the core Flink distribution.
FLINK_DIR="/opt/${FLINK_BASE}"
LIB_DIR="$FLINK_DIR/lib/"
OPT_DIR="$FLINK_DIR/opt/"

cp ./flink-connectors/flink-sql-connector-kafka/target/flink-sql-connector-kafka-1.15.0.jar $LIB_DIR
cp ./flink-table/flink-table-runtime/target/flink-table-runtime-1.15.0.jar  $LIB_DIR
cp ./flink-metrics/flink-metrics-prometheus/target/flink-metrics-prometheus-1.15.0.jar $LIB_DIR

cp ./flink-connectors/flink-connector-hive/target/flink-connector-hive_2.12-1.15.0.jar $OPT_DIR

aws s3 cp s3://dev-dataflo/ops/ec2/flink-lib/libfb303-0.9.3.jar $LIB_DIR
aws s3 cp s3://dev-dataflo/ops/ec2/flink-lib/hive-exec-3.1.2.jar $LIB_DIR

mkdir -p $FLINK_DIR/plugins/s3-fs-presto
cp ./flink-filesystems/flink-s3-fs-presto/target/flink-s3-fs-presto-1.15.0.jar $FLINK_DIR/plugins/s3-fs-presto/

# Build a Flink binary.
echo "Building Flink binary at /opt/${FLINK_BASE}.tar.gz"
find /opt/${FLINK_BASE}/log -type f | xargs rm -f
pushd /opt
rm -f ${FLINK_BASE}.tar.gz
tar zcfh ${FLINK_BASE}.tar.gz ${FLINK_BASE}
popd