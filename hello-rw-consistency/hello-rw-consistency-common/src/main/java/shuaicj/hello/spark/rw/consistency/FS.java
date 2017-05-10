package shuaicj.hello.spark.rw.consistency;

import java.io.IOException;
import java.io.Serializable;

/**
 * Ops for distributed file system.
 *
 * @author shuaicj 2017/05/09
 */
public interface FS extends Serializable {

    byte[] read(String path) throws IOException;

    void write(String path, byte[] bytes) throws IOException;

    boolean exists(String path) throws IOException;

    boolean delete(String path) throws IOException;
}
