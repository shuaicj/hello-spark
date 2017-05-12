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
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.util.StringUtils;

import java.util.List;

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
            } else if (match(args, "--type s3 --access-key * --secret-key * --endpoint * --create *")) {
                S3.conn(args[3], args[5], args[7]).createBucket(args[9]);
            } else if (match(args, "--type s3 --access-key * --secret-key * --endpoint * --ls")) {
                S3.conn(args[3], args[5], args[7]).ls();
            } else if (match(args, "--type s3 --access-key * --secret-key * --endpoint * --ls *")) {
                S3.conn(args[3], args[5], args[7]).lsBucket(args[9]);
            } else if (match(args, "--type s3 --access-key * --secret-key * --endpoint * --ls * *")) {
                S3.conn(args[3], args[5], args[7]).lsObject(args[9], args[10]);
            } else if (match(args, "--type s3 --access-key * --secret-key * --endpoint * --rm *")) {
                S3.conn(args[3], args[5], args[7]).rmBucket(args[9]);
            } else if (match(args, "--type s3 --access-key * --secret-key * --endpoint * --rm * *")) {
                S3.conn(args[3], args[5], args[7]).rmObject(args[9], args[10]);
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

        public void lsObject(String bucket, String object) {
            ObjectMetadata meta = conn.getObjectMetadata(bucket, object);
            System.out.println(object + "\t" + meta.getContentLength() + "\t" +
                    StringUtils.fromDate(meta.getLastModified()));
        }

        public void rmBucket(String bucket) throws InterruptedException {
            ObjectListing objects = conn.listObjects(bucket);
            do {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                    conn.deleteObject(bucket, objectSummary.getKey());
                }
                objects = conn.listNextBatchOfObjects(objects);
            } while (objects.isTruncated());

            VersionListing versions = conn.listVersions(new ListVersionsRequest().withBucketName(bucket));
            do {
                for (S3VersionSummary versionSummary : versions.getVersionSummaries()) {
                    conn.deleteVersion(bucket, versionSummary.getKey(), versionSummary.getVersionId());
                }
                versions = conn.listNextBatchOfVersions(versions);
            } while (versions.isTruncated());
            conn.deleteBucket(bucket);
        }

        public void rmObject(String bucket, String object) {
            conn.deleteObject(bucket, object);
        }
    }
}
