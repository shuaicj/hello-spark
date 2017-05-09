package shuaicj.hello.spark.rw.consistency;

/**
 * Verify r/w consistency on local path.
 * Mount Ceph or Samba as a local path on each spark slave.
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
