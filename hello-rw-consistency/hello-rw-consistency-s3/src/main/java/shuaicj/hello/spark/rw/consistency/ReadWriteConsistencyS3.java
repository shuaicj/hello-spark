package shuaicj.hello.spark.rw.consistency;

/**
 * Verify r/w consistency via Amazon S3 API.
 * Support any file system which is compatible with S3 API, like AWS or Ceph.
 *
 * @author shuaicj 2017/05/09
 */
public class ReadWriteConsistencyS3 {

    public static void main(String[] args) throws Exception {
        if (args.length != 12
                || !args[0].equals("--dir")
                || !args[2].equals("--num") || Integer.parseInt(args[3]) <= 0
                || !args[4].equals("--size") || Integer.parseInt(args[5]) <= 0
                || !args[6].equals("--access-key")
                || !args[8].equals("--secret-key")
                || !args[10].equals("--endpoint")) {
            throw new IllegalArgumentException("dir, num, size, access key, secret key, endpoint required");
        }
        final String dir = args[1];
        final int num = Integer.parseInt(args[3]);
        final int size = Integer.parseInt(args[5]);
        final String accessKey = args[7];
        final String secretKey = args[9];
        final String endpoint = args[11];

        final FS fs = new S3FS(accessKey, secretKey, endpoint);
        final ConsistencyChecker checker = new ConsistencyChecker(fs, dir, num, size);
        checker.check();
    }
}
