package shuaicj.hello.spark.rw.consistency;

import java.io.IOException;
import java.io.Serializable;

/**
 * Message Digest, to calculate the md5 of a file.
 *
 * @author shuaicj 2017/05/22
 */
public interface MD5er extends Serializable {
    String md5(String file) throws IOException;
}
