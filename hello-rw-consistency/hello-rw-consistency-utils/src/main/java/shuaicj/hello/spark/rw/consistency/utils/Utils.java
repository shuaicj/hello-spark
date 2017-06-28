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

import java.io.File;
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
            } else if (match(args, "--type s3 --access-key * --secret-key * --endpoint * --get * * *")) {
                S3.conn(args[3], args[5], args[7]).getObject(args[9], args[10], args[11]);
            } else if (match(args, "--type s3 --access-key * --secret-key * --endpoint * --put * * *")) {
                S3.conn(args[3], args[5], args[7]).putObject(args[9], args[10], args[11]);
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
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
                    .build());

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

        public void rmBucket(String bucket) throws InterruptedException {
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
