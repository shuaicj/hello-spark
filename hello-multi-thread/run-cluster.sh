#!/bin/bash 
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

spark-submit \
    --class shuaicj.hello.spark.multi.thread.MultiThreadApp \
    --master spark://HOST:PORT \
    $DIR/target/hello-multi-thread-1.0.0.jar
