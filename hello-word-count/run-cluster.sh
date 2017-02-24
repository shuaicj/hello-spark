#!/bin/bash 
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

spark-submit \
    --class shuaicj.hello.spark.word.count.WordCountApp \
    --master spark://HOST:PORT \
    $DIR/target/hello-word-count-1.0.0.jar \
    $DIR/src/test/resources/words.txt
