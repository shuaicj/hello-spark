package shuaicj.hello.spark.rw.consistency.utils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.util.StringUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.util.List;

/**
 * Useful helper.
 *
 * @author shuaicj 2017/05/11
 */
@SuppressWarnings("AccessStaticViaInstance")
public class Utils {

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("access-key").isRequired().hasArg().withDescription("s3 access key").create());
        options.addOption(OptionBuilder.withLongOpt("secret-key").isRequired().hasArg().withDescription("s3 secret key").create());
        options.addOption(OptionBuilder.withLongOpt("endpoint").hasArg().withDescription("s3 endpoint").create());
        options.addOption(OptionBuilder.withLongOpt("region").hasArg().withDescription("s3 region").create());
        options.addOption(OptionBuilder.withLongOpt("create").hasArg().withDescription("create [bucket]").create());
        options.addOption(OptionBuilder.withLongOpt("ls").hasOptionalArgs(2).withDescription("ls | ls [bucket] | ls [bucket] [objectkey]").create());
        options.addOption(OptionBuilder.withLongOpt("rm").hasOptionalArgs(2).withDescription("rm [bucket] | rm [bucket] [objectkey]").create());
        options.addOption(OptionBuilder.withLongOpt("get").hasArgs(3).withDescription("get [bucket] [objectkey] [localpath]").create());
        options.addOption(OptionBuilder.withLongOpt("put").hasArgs(3).withDescription("put [bucket] [objectkey] [localpath]").create());

        try {
            CommandLineParser parser = new BasicParser();
            CommandLine cli = parser.parse(options, args);

            final String ak = cli.getOptionValue("access-key");
            final String sk = cli.getOptionValue("secret-key");
            final String ep = cli.getOptionValue("endpoint");
            final String rg = cli.getOptionValue("region");

            if (cli.hasOption("create")) {
                final String[] vv = cli.getOptionValues("create");
                S3.conn(ak, sk, ep, rg).createBucket(vv[0]);
            } else if (cli.hasOption("ls")) {
                final String[] vv = cli.getOptionValues("ls");
                if (vv == null) {
                    S3.conn(ak, sk, ep, rg).ls();
                } else if (vv.length == 1) {
                    S3.conn(ak, sk, ep, rg).lsBucket(vv[0]);
                } else if (vv.length == 2) {
                    S3.conn(ak, sk, ep, rg).lsObject(vv[0], vv[1]);
                }
            } else if (cli.hasOption("rm")) {
                final String[] vv = cli.getOptionValues("rm");
                if (vv.length == 1) {
                    S3.conn(ak, sk, ep, rg).rmBucket(vv[0]);
                } else if (vv.length == 2) {
                    S3.conn(ak, sk, ep, rg).rmObject(vv[0], vv[1]);
                }
            } else if (cli.hasOption("get")) {
                final String[] vv = cli.getOptionValues("get");
                S3.conn(ak, sk, ep, rg).getObject(vv[0], vv[1], vv[2]);
            } else if (cli.hasOption("put")) {
                final String[] vv = cli.getOptionValues("put");
                S3.conn(ak, sk, ep, rg).putObject(vv[0], vv[1], vv[2]);
            } else {
                printUsage(options);
            }
        } catch (ParseException | IndexOutOfBoundsException | NullPointerException e) {
            printUsage(options);
        }
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar path-to-jar", options);
    }

    private static class S3 {

        private final AmazonS3 conn;

        private S3(AmazonS3 conn) {
            this.conn = conn;
        }

        public static S3 conn(String accessKey, String secretKey, String endpoint, String region) {
            AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                    .withClientConfiguration(new ClientConfiguration().withProtocol(Protocol.HTTP));

            if (endpoint != null) {
                return new S3(builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        endpoint, region == null ? "us-east-1" : region)).build());
            }
            return new S3(builder.build());

            // AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            //
            // ClientConfiguration clientConfig = new ClientConfiguration();
            // clientConfig.setProtocol(Protocol.HTTP);
            //
            // AmazonS3 conn = new AmazonS3Client(credentials, clientConfig);
            // conn.setEndpoint(endpoint);
            //
            // return new S3(conn);
        }

        public void createBucket(String bucket) {
            conn.createBucket(bucket);
        }

        public void ls() {
            System.out.println("buckets {");
            List<Bucket> buckets = conn.listBuckets();
            for (Bucket bucket : buckets) {
                System.out.println(bucket.getName() + "\t" +
                        StringUtils.fromDate(bucket.getCreationDate()));
            }
            System.out.println("}");
        }

        public void lsBucket(String bucket) {
            System.out.println(bucket + " objects {");
            ObjectListing objects = conn.listObjects(bucket);
            long count = 0;
            while (true) {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                    count++;
                    System.out.println(objectSummary.getKey() + "\t" +
                            objectSummary.getSize() + "\t" + objectSummary.getETag() + "\t" +
                            StringUtils.fromDate(objectSummary.getLastModified()));
                }
                if (objects.isTruncated()) {
                    objects = conn.listNextBatchOfObjects(objects);
                } else {
                    break;
                }
            }
            System.out.println("}");
            System.out.println("object count: " + count);
        }

        public void lsObject(String bucket, String object) {
            ObjectMetadata meta = conn.getObjectMetadata(bucket, object);
            System.out.println(object + "\t" + meta.getContentLength() + "\t" + meta.getETag() + "\t" +
                    StringUtils.fromDate(meta.getLastModified()));
        }

        public void rmBucket(String bucket) {
            ObjectListing objects = conn.listObjects(bucket);
            while (true) {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                    conn.deleteObject(bucket, objectSummary.getKey());
                }
                if (objects.isTruncated()) {
                    objects = conn.listNextBatchOfObjects(objects);
                } else {
                    break;
                }
            }

            VersionListing versions = conn.listVersions(new ListVersionsRequest().withBucketName(bucket));
            while (true) {
                for (S3VersionSummary versionSummary : versions.getVersionSummaries()) {
                    conn.deleteVersion(bucket, versionSummary.getKey(), versionSummary.getVersionId());
                }
                if (versions.isTruncated()) {
                    versions = conn.listNextBatchOfVersions(versions);
                } else {
                    break;
                }
            }
            conn.deleteBucket(bucket);
        }

        public void rmObject(String bucket, String object) {
            conn.deleteObject(bucket, object);
        }

        public void getObject(String bucket, String object, String local) {
            conn.getObject(new GetObjectRequest(bucket, object), new File(local));
        }

        public void putObject(String bucket, String object, String local) {
            conn.putObject(bucket, object, new File(local));
        }
    }
}
