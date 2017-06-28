package shuaicj.hello.spark.rw.consistency;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import shuaicj.hello.spark.rw.consistency.md5.StandardMD5er;

/**
 * Verify r/w consistency via Amazon S3 API.
 * Support any file system which is compatible with S3 API, like AWS or Ceph.
 *
 * @author shuaicj 2017/05/09
 */
@SuppressWarnings("AccessStaticViaInstance")
public class ReadWriteConsistencyS3 {

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("dir").isRequired().hasArg().withDescription("work directory").create());
        options.addOption(OptionBuilder.withLongOpt("num").isRequired().hasArg().withDescription("file number").create());
        options.addOption(OptionBuilder.withLongOpt("size").isRequired().hasArg().withDescription("file size").create());
        options.addOption(OptionBuilder.withLongOpt("access-key").isRequired().hasArg().withDescription("s3 access key").create());
        options.addOption(OptionBuilder.withLongOpt("secret-key").isRequired().hasArg().withDescription("s3 secret key").create());
        options.addOption(OptionBuilder.withLongOpt("endpoint").hasArg().withDescription("s3 endpoint").create());
        options.addOption(OptionBuilder.withLongOpt("region").hasArg().withDescription("s3 region").create());

        CommandLineParser parser = new BasicParser();
        CommandLine cli = parser.parse(options, args);

        final String dir = cli.getOptionValue("dir");
        final int num = Integer.parseInt(cli.getOptionValue("num"));
        final int size = Integer.parseInt(cli.getOptionValue("size"));
        final String accessKey = cli.getOptionValue("access-key");
        final String secretKey = cli.getOptionValue("secret-key");
        final String endpoint = cli.getOptionValue("endpoint");
        final String region = cli.getOptionValue("region");

        final FS fs = new S3FS(accessKey, secretKey, endpoint, region);
        final MD5er md5er = new StandardMD5er(fs);
        final ConsistencyChecker checker = new ConsistencyChecker(fs, md5er, dir, num, size);
        checker.check();
    }
}
