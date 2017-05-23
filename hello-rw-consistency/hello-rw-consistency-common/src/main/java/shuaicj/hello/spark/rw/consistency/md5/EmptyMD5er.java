package shuaicj.hello.spark.rw.consistency.md5;

import shuaicj.hello.spark.rw.consistency.MD5er;

import java.io.IOException;

/**
 * Do nothing, just return a fixed string.
 *
 * @author shuaicj 2017/05/23
 */
@SuppressWarnings("serial")
public class EmptyMD5er implements MD5er {

    @Override
    public String md5(String file) throws IOException {
        return "EmptyMD5";
    }
}
