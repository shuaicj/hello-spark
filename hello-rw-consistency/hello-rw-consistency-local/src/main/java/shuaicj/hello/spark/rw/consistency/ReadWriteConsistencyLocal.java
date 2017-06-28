package shuaicj.hello.spark.rw.consistency;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import shuaicj.hello.spark.rw.consistency.md5.ExternalMD5er;
import shuaicj.hello.spark.rw.consistency.md5.SequenceMD5er;
import shuaicj.hello.spark.rw.consistency.md5.SlowMD5er;
import shuaicj.hello.spark.rw.consistency.md5.StandardMD5er;

/**
 * Verify r/w consistency on local path.
 * Support any file system which is capable of being mounted as a local path on each
 * spark slave, like Ceph or Samba.
 *
 * @author shuaicj 2017/05/09
 */
@SuppressWarnings("AccessStaticViaInstance")
public class ReadWriteConsistencyLocal {

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("dir").isRequired().hasArg().withDescription("work directory").create());
        options.addOption(OptionBuilder.withLongOpt("num").isRequired().hasArg().withDescription("file number").create());
        options.addOption(OptionBuilder.withLongOpt("size").isRequired().hasArg().withDescription("file size").create());

        CommandLineParser parser = new BasicParser();
        CommandLine cli = parser.parse(options, args);

        final String dir = cli.getOptionValue("dir");
        final int num = Integer.parseInt(cli.getOptionValue("num"));
        final int size = Integer.parseInt(cli.getOptionValue("size"));

        final FS fs = new LocalFS();
        MD5er md5er;
        if (args.length == 6) {
            md5er = new StandardMD5er(fs);
        } else if (args.length == 8 && args[6].equals("--md5er") && args[7].equals("slow")) {
            md5er = new SlowMD5er();
        } else if (args.length == 8 && args[6].equals("--md5er")) {
            md5er = new SequenceMD5er(new StandardMD5er(fs), new ExternalMD5er(args[7]), new StandardMD5er(fs));
        } else {
            throw new IllegalArgumentException("invalid md5er");
        }

        final ConsistencyChecker checker = new ConsistencyChecker(fs, md5er, dir, num, size);
        checker.check();
    }
}
