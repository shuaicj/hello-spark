package shuaicj.hello.spark.rw.consistency.md5;

import shuaicj.hello.spark.rw.consistency.MD5er;

import javax.xml.bind.DatatypeConverter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Emulate a slow way to calculate md5.
 *
 * @author shuaicj 2017/05/22
 */
@SuppressWarnings("serial")
public class SlowMD5er implements MD5er {

    @Override
    public String md5(String file) throws IOException {
        try (InputStream in = new FileInputStream(file)) {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] buf = new byte[4096];
            for (int len = in.read(buf); len != -1; len = in.read(buf)) {
                md.update(buf, 0, len);
                Thread.sleep(100);
            }
            return DatatypeConverter.printHexBinary(md.digest()).toLowerCase();
        } catch (InterruptedException | NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }
}
