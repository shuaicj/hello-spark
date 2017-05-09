package shuaicj.hello.spark.rw.consistency;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

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
    public InputStream inputStream(String path) throws IOException {
        Path p = new Path(path);
        return p.getFileSystem(new Configuration()).open(p);
    }

    @Override
    public OutputStream outputStream(String path) throws IOException {
        Path p = new Path(path);
        return p.getFileSystem(new Configuration()).create(p);
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
