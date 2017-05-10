# hello-spark
Hello Spark!
- **hello-word-count** - word count via `Spark MapReduce`
- **hello-word-count-network** - word count via `Spark Stream`
- **hello-parallelize** - usage of `SparkContext.parallelize()`
- **hello-multi-thread** - submit tasks in multi threads
- **hello-rw-consistency** - test read write consistency on different distributed file systems
    - **hello-rw-concistency-common** - common interface and process
    - **hello-rw-concistency-local** - test any file system which is capable of being mounted as a local path on each spark slave, like `Ceph` or `Samba`
    - **hello-rw-concistency-hdfs** - test `Apache HDFS`
    - **hello-rw-concistency-s3** - test any file system which is compatible with `S3 API`, like `AWS` or `Ceph`
