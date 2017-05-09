package shuaicj.hello.spark.rw.consistency;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Ops for distributed file system.
 *
 * @author shuaicj 2017/05/09
 */
public interface FS extends Serializable {

    InputStream inputStream(String path) throws IOException;

    OutputStream outputStream(String path) throws IOException;

    boolean exists(String path) throws IOException;

    boolean delete(String path) throws IOException;
}
