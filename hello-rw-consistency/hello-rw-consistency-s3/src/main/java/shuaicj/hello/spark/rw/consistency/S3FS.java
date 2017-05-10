package shuaicj.hello.spark.rw.consistency;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import lombok.Getter;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.springframework.util.DigestUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Any FS which supports S3 API.
 * A path is like /bucket-name/object-key
 *
 * @author shuaicj 2017/05/10
 */
@SuppressWarnings("serial")
public class S3FS implements FS {

    private final String accessKey;
    private final String secretKey;
    private final String endpoint;

    public S3FS(String accessKey, String secretKey, String endpoint) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpoint = endpoint;
    }

    @Override
    public byte[] read(String path) throws IOException {
        Path p = Path.from(path);
        byte[] content;
        ObjectMetadata meta;
        try (S3Object obj = conn().getObject(p.getBucket(), p.getObjectKey())) {
            content = IOUtils.toByteArray(obj.getObjectContent());
            meta = obj.getObjectMetadata();
        } catch (Exception e) {
            throw new IOException(e);
        }
        if (meta.getContentLength() != content.length) {
            throw new IOException("length not same! expect " + meta.getContentLength() + " actual " + content.length);
        }
        // md5 is in getETag() and is a hex string
        String md5 = DigestUtils.md5DigestAsHex(content);
        if (!meta.getETag().equals(md5)) {
            throw new IOException("md5 not same! expect " + meta.getETag() + " actual " + md5);
        }
        return content;
    }

    @Override
    public void write(String path, byte[] bytes) throws IOException {
        Path p = Path.from(path);
        try (InputStream input = new ByteArrayInputStream(bytes)) {
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(bytes.length);
            meta.setContentMD5(new String(Base64.encodeBase64(DigestUtils.md5Digest(bytes))));
            AmazonS3 conn = conn();
            createBucket(conn, p.getBucket());
            conn.putObject(p.getBucket(), p.getObjectKey(), input, meta);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean exists(String path) throws IOException {
        Path p = Path.from(path);
        try {
            AmazonS3 conn = conn();
            return conn.doesBucketExist(p.getBucket()) && conn.doesObjectExist(p.getBucket(), p.getObjectKey());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean delete(String path) throws IOException {
        Path p = Path.from(path);
        try {
            AmazonS3 conn = conn();
            if (conn.doesBucketExist(p.getBucket())) {
                conn.deleteObject(p.getBucket(), p.getObjectKey());
            }
            return true;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private AmazonS3 conn() {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withClientConfiguration(new ClientConfiguration().withProtocol(Protocol.HTTP))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, Regions.DEFAULT_REGION.getName()))
                .build();
    }

    private boolean createBucket(AmazonS3 conn, String bucket) {
        int retry = 3;
        for (int i = 0; i < retry; i++) {
            try {
                if (!conn.doesBucketExist(bucket)) {
                    conn.createBucket(bucket);
                }
                return true;
            } catch (Exception e) {
                if (i == retry - 1) {
                    throw e;
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    @Getter
    public static final class Path {

        private final String bucket, objectKey;

        private Path(String bucket, String objectKey) {
            this.bucket = bucket;
            this.objectKey = objectKey;
        }

        public static Path from(String path) {
            String[] ss = path.split("/");
            if (ss.length != 3 || !ss[0].isEmpty() || ss[1].isEmpty() || ss[2].isEmpty()) {
                throw new IllegalArgumentException(path);
            }
            return new Path(ss[1], ss[2]);
        }

        public static String concat(String bucket, String objectKey) {
            if (bucket.isEmpty() || objectKey.isEmpty() || bucket.contains("/") || objectKey.contains("/")) {
                throw new IllegalArgumentException("bucket or objectKey empty");
            }
            return "/" + bucket + "/" + objectKey;
        }
    }
}
