#!/bin/bash 
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

spark-submit \
    --name ReadWriteConsistencyHdfs \
    --class shuaicj.hello.spark.rw.consistency.ReadWriteConsistencyHdfs \
    --master spark://SPARK-HOST:SPARK-PORT \
    --executor-cores 1 \
    --executor-memory 1200M \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.executorIdleTimeout=30s \
    $DIR/hello-rw-consistency-hdfs-1.0.0.jar \
    --dir hdfs://HDFS-HOST:HDFS-PORT/my/test/dir \
    --num 36 \
    --size 10000000
