#!/bin/bash 
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

spark-submit \
    --name ReadWriteConsistencyLocal \
    --class shuaicj.hello.spark.rw.consistency.ReadWriteConsistencyLocal \
    --master spark://SPARK-HOST:SPARK-PORT \
    --executor-cores 1 \
    --executor-memory 1200M \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.executorIdleTimeout=30s \
    $DIR/hello-rw-consistency-local-1.0.0.jar \
    --dir /my/test/dir \
    --num 36 \
    --size 10000000 \
    --md5er "md5cmd -cpp"
