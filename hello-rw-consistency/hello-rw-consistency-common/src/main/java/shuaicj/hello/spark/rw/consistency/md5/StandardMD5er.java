package shuaicj.hello.spark.rw.consistency.md5;

import org.springframework.util.DigestUtils;
import shuaicj.hello.spark.rw.consistency.FS;
import shuaicj.hello.spark.rw.consistency.MD5er;

import java.io.IOException;

/**
 * The normal way to calculate md5.
 *
 * @author shuaicj 2017/05/22
 */
@SuppressWarnings("serial")
public class StandardMD5er implements MD5er {

    private final FS fs;

    public StandardMD5er(FS fs) {
        this.fs = fs;
    }

    @Override
    public String md5(String file) throws IOException {
        return DigestUtils.md5DigestAsHex(fs.read(file));
    }
}
