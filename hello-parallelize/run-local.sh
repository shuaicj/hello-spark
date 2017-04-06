#!/bin/bash 
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

spark-submit \
    --class shuaicj.hello.spark.parallelize.ParallelizeApp \
    --master local[4] \
    $DIR/target/hello-parallelize-1.0.0.jar
