package shuaicj.hello.spark.rw.consistency;

/**
 * Verify r/w consistency on Ceph local storage.
 * The distributed file system - Ceph, is mount as a local path on each spark slave.
 *
 * @author shuaicj 2017/05/09
 */
public class ReadWriteConsistencyLocal {

    public static void main(String[] args) throws Exception {
        final FS fs = new LocalFS();
        final ConsistencyChecker checker = new ConsistencyChecker(args, fs);
        checker.check();
    }
}
