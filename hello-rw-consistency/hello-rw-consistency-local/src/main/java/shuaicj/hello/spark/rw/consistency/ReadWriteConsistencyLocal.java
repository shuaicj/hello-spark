package shuaicj.hello.spark.rw.consistency;

import shuaicj.hello.spark.rw.consistency.md5.ExternalMD5er;
import shuaicj.hello.spark.rw.consistency.md5.SlowMD5er;
import shuaicj.hello.spark.rw.consistency.md5.StandardMD5er;

/**
 * Verify r/w consistency on local path.
 * Support any file system which is capable of being mounted as a local path on each
 * spark slave, like Ceph or Samba.
 *
 * @author shuaicj 2017/05/09
 */
public class ReadWriteConsistencyLocal {

    public static void main(String[] args) throws Exception {
        if (args.length < 6
                || !args[0].equals("--dir")
                || !args[2].equals("--num") || Integer.parseInt(args[3]) <= 0
                || !args[4].equals("--size") || Integer.parseInt(args[5]) <= 0) {
            throw new IllegalArgumentException("dir, num and size required");
        }
        final String dir = args[1];
        final int num = Integer.parseInt(args[3]);
        final int size = Integer.parseInt(args[5]);

        MD5er md5er;
        if (args.length == 6) {
            md5er = new StandardMD5er();
        } else if (args.length == 8 && args[6].equals("--md5er") && args[7].equals("slow")) {
            md5er = new SlowMD5er();
        } else if (args.length == 8 && args[6].equals("--md5er")) {
            md5er = new ExternalMD5er(args[7]);
        } else {
            throw new IllegalArgumentException("invalid md5er");
        }

        final FS fs = new LocalFS();
        final ConsistencyChecker checker = new ConsistencyChecker(fs, md5er, dir, num, size);
        checker.check();
    }
}
