package shuaicj.hello.spark.rw.consistency;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import shuaicj.hello.spark.rw.consistency.md5.StandardMD5er;

/**
 * Verify r/w consistency on HDFS.
 *
 * @author shuaicj 2017/05/04
 */
@SuppressWarnings("AccessStaticViaInstance")
public class ReadWriteConsistencyHdfs {

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

        final FS fs = new HDFS();
        final MD5er md5er = new StandardMD5er(fs);
        final ConsistencyChecker checker = new ConsistencyChecker(fs, md5er, dir, num, size);
        checker.check();
    }
}

