# hello-spark
Hello Spark!
- **hello-word-count** - word count via `Spark MapReduce`
- **hello-word-count-network** - word count via `Spark Stream`
- **hello-parallelize** - usage of `SparkContext.parallelize()`
- **hello-multi-thread** - submit tasks in multi threads
- **hello-rw-consistency** - test read write consistency on different distributed file systems
    - **hello-rw-concistency-common** - common interface and process
    - **hello-rw-concistency-local** - mount `Ceph` or `Samba` as local path on each spark slave
    - **hello-rw-concistency-hdfs** - `Apache HDFS`
