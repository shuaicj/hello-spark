package shuaicj.hello.spark.rw.consistency;

import org.apache.commons.io.IOUtils;

import java.io.BufferedOutputStream;
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
    public byte[] read(String path) throws IOException {
        try (InputStream in = new FileInputStream(path)) {
            return IOUtils.toByteArray(in);
        }
    }

    @Override
    public void write(String path, byte[] bytes) throws IOException {
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(path))) {
            out.write(bytes);
        }
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
