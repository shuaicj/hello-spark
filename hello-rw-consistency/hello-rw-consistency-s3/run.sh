#!/bin/bash 
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

spark-submit \
    --name ReadWriteConsistencyS3 \
    --class shuaicj.hello.spark.rw.consistency.ReadWriteConsistencyS3 \
    --master spark://SPARK-HOST:SPARK-PORT \
    --executor-cores 1 \
    --executor-memory 1200M \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.executorIdleTimeout=30s \
    $DIR/hello-rw-consistency-s3-1.0.0.jar \
    --dir /bucket \
    --num 36 \
    --size 10000000 \
    --access-key YOUR_ACCESS_KEY \
    --secret-key YOUR_SECRET_KEY \
    --endpoint YOUR_ENDPOINT \
    --region YOUR_REGION
