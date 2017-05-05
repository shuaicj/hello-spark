#!/bin/bash 
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

spark-submit \
    --name ReadWriteConsistencyLocal \
    --class shuaicj.hello.spark.rw.consistency.ReadWriteConsistencyLocal \
    --master spark://HOST:PORT \
    --executor-cores 1 \
    --executor-memory 1200M \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.executorIdleTimeout=30s \
    $DIR/hello-rw-consistency-local-1.0.0.jar \
    --dir DIR_READ_WRITE \
    --num FILE_NUM \
    --size FILE_SIZE
