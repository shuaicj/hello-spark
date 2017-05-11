package shuaicj.hello.spark.rw.consistency.utils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Useful helper.
 *
 * @author shuaicj 2017/05/11
 */
public class Utils {

    public static void main(String[] args) throws Exception {
        try {
            if (match(args, "--version")) {
                System.out.println("1.0.0");
            } else if (match(args, "--type s3 --access-key * --secret-key * --endpoint * --l")) {
                S3.conn(args[3], args[5], args[7]).list();
            } else if (match(args, "--type s3 --access-key * --secret-key * --endpoint * --lb")) {
                S3.conn(args[3], args[5], args[7]).listBuckets();
            } else if (match(args, "--type s3 --access-key * --secret-key * --endpoint * --lo *")) {
                S3.conn(args[3], args[5], args[7]).listObjects(args[9]);
            } else {
                throw new IllegalArgumentException("args illegal");
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("more args required");
        }
    }

    private static boolean match(String[] args, String pattern) {
        String[] ss = pattern.split(" ");
        if (args.length == ss.length) {
            for (int i = 0; i < args.length; i++) {
                if (!ss[i].equals("*") && !ss[i].equals(args[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private static class S3 {

        private final AmazonS3 conn;

        private S3(AmazonS3 conn) {
            this.conn = conn;
        }

        public static S3 conn(String accessKey, String secretKey, String endpoint) {
            return new S3(AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                    .withClientConfiguration(new ClientConfiguration().withProtocol(Protocol.HTTP))
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, Regions.DEFAULT_REGION.getName()))
                    .build());
        }

        public void list() {
            List<String> buckets = listBuckets();
            for (String b : buckets) {
                listObjects(b);
            }
        }

        public List<String> listBuckets() {
            System.out.println("buckets {");
            List<Bucket> buckets = conn.listBuckets();
            for (Bucket bucket : buckets) {
                System.out.println(bucket.getName() + "\t" +
                        StringUtils.fromDate(bucket.getCreationDate()));
            }
            System.out.println("}");
            return buckets.stream().map(Bucket::getName).collect(Collectors.toList());
        }

        public void listObjects(String bucket) {
            System.out.println(bucket + " objects {");
            ObjectListing objects = conn.listObjects(bucket);
            do {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                    System.out.println(objectSummary.getKey() + "\t" +
                            objectSummary.getSize() + "\t" +
                            StringUtils.fromDate(objectSummary.getLastModified()));
                }
                objects = conn.listNextBatchOfObjects(objects);
            } while (objects.isTruncated());
            System.out.println("}");
        }
    }
}
