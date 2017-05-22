#!/bin/bash 
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

CMD=md5cmd

g++ -o $DIR/$CMD $DIR/main.cpp $DIR/md5.c

# mv $DIR/$CMD /usr/local/bin/$CMD
