#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

spark-submit \
    --class shuaicj.hello.spark.slave.ip.SlaveIpApp \
    --master spark://HOST:PORT \
    $DIR/target/hello-slave-ip-1.0.0.jar
