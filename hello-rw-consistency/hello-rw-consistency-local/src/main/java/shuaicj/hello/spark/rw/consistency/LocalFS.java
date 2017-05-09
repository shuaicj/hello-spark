package shuaicj.hello.spark.rw.consistency;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Local FS uses local path.
 *
 * @author shuaicj 2017/05/09
 */
@SuppressWarnings("serial")
public class LocalFS implements FS {

    @Override
    public InputStream inputStream(String path) throws IOException {
        return new FileInputStream(path);
    }

    @Override
    public OutputStream outputStream(String path) throws IOException {
        return new FileOutputStream(path);
    }

    @Override
    public boolean exists(String path) throws IOException {
        return new File(path).exists();
    }

    @Override
    public boolean delete(String path) throws IOException {
        return new File(path).delete();
    }
}
