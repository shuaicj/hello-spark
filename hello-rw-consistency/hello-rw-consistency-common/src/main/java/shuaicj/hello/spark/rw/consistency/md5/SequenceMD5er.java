package shuaicj.hello.spark.rw.consistency.md5;

import shuaicj.hello.spark.rw.consistency.MD5er;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Concat the output of multiple MD5er.
 *
 * @author shuaicj 2017/05/22
 */
@SuppressWarnings("serial")
public class SequenceMD5er implements MD5er {

    private final List<MD5er> md5ers;

    public SequenceMD5er(MD5er... md5ers) {
        this.md5ers = Arrays.asList(md5ers);
    }

    @Override
    public String md5(String file) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (MD5er md5er : md5ers) {
            sb.append('(');
            sb.append(md5er.getClass().getSimpleName());
            sb.append(':');
            sb.append(md5er.md5(file));
            sb.append(')');
        }
        return sb.toString();
    }
}
