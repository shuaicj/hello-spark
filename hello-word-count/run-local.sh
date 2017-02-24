#!/bin/bash 
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

spark-submit \
    --class shuaicj.hello.spark.word.count.WordCountApp \
    --master local[4] \
    $DIR/target/hello-word-count-1.0.0.jar \
    $DIR/src/test/resources/words.txt
