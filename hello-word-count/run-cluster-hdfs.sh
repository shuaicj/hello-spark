#!/bin/bash 
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

spark-submit \
    --class shuaicj.hello.spark.word.count.WordCountApp \
    --master spark://SPARK_HOST:SPARK_PORT \
    $DIR/target/hello-word-count-1.0.0.jar \
    hdfs://HDFS_HOST:HDFS_PORT/test/resources/words.txt
