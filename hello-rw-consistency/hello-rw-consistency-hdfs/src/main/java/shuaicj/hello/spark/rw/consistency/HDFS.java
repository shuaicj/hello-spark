package shuaicj.hello.spark.rw.consistency;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Use Apache HDFS.
 *
 * @author shuaicj 2017/05/09
 */
@SuppressWarnings("serial")
public class HDFS implements FS {

    @Override
    public byte[] read(String path) throws IOException {
        Path p = new Path(path);
        try (InputStream in = p.getFileSystem(new Configuration()).open(p)) {
            return IOUtils.toByteArray(in);
        }
    }

    @Override
    public void write(String path, byte[] bytes) throws IOException {
        Path p = new Path(path);
        try (OutputStream out = new BufferedOutputStream(p.getFileSystem(new Configuration()).create(p))) {
            out.write(bytes);
        }
    }

    @Override
    public boolean exists(String path) throws IOException {
        Path p = new Path(path);
        return p.getFileSystem(new Configuration()).exists(p);
    }

    @Override
    public boolean delete(String path) throws IOException {
        Path p = new Path(path);
        return p.getFileSystem(new Configuration()).delete(p, false);
    }
}
