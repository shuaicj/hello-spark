#!/bin/bash 
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

java -jar \
    $DIR/hello-rw-consistency-utils-1.0.0.jar \
    --access-key YOUR_ACCESS_KEY \
    --secret-key YOUR_SECRET_KEY \
    --endpoint YOUR_ENDPOINT \
    --region YOUR_REGION \
    $@
