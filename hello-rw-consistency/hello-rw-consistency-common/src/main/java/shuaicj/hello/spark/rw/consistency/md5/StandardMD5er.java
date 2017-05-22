package shuaicj.hello.spark.rw.consistency.md5;

import org.springframework.util.DigestUtils;
import shuaicj.hello.spark.rw.consistency.MD5er;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * The normal way to calculate md5.
 *
 * @author shuaicj 2017/05/22
 */
@SuppressWarnings("serial")
public class StandardMD5er implements MD5er {

    @Override
    public String md5(String file) throws IOException {
        return DigestUtils.md5DigestAsHex(new FileInputStream(file));
    }
}
