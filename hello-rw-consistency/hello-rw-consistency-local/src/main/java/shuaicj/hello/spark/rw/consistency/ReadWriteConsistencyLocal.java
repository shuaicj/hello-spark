package shuaicj.hello.spark.rw.consistency;

/**
 * Verify r/w consistency on local path.
 * Support any file system which is capable of being mounted as a local path on each
 * spark slave, like Ceph or Samba.
 *
 * @author shuaicj 2017/05/09
 */
public class ReadWriteConsistencyLocal {

    public static void main(String[] args) throws Exception {
        if (args.length != 6
                || !args[0].equals("--dir")
                || !args[2].equals("--num") || Integer.parseInt(args[3]) <= 0
                || !args[4].equals("--size") || Integer.parseInt(args[5]) <= 0) {
            throw new IllegalArgumentException("dir, num and size required");
        }
        final String dir = args[1];
        final int num = Integer.parseInt(args[3]);
        final int size = Integer.parseInt(args[5]);

        final FS fs = new LocalFS();
        final ConsistencyChecker checker = new ConsistencyChecker(fs, dir, num, size);
        checker.check();
    }
}
