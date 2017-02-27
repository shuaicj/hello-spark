#!/bin/bash 
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

spark-submit \
    --class shuaicj.hello.spark.word.count.network.WordCountNetwork \
    --master local[4] \
    $DIR/target/hello-word-count-network-1.0.0.jar \
    localhost 9999
