package shuaicj.hello.spark.rw.consistency;

/**
 * Verify r/w consistency on HDFS.
 *
 * @author shuaicj 2017/05/04
 */
public class ReadWriteConsistencyHdfs {

    public static void main(String[] args) throws Exception {
        final FS fs = new HDFS();
        final ConsistencyChecker checker = new ConsistencyChecker(args, fs);
        checker.check();
    }
}

