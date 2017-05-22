package shuaicj.hello.spark.rw.consistency.md5;

import shuaicj.hello.spark.rw.consistency.MD5er;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Call the external process to calculate md5.
 *
 * @author shuaicj 2017/05/22
 */
@SuppressWarnings("serial")
public class ExternalMD5er implements MD5er {

    private final String cmd;

    public ExternalMD5er(String cmd) {
        this.cmd = cmd;
    }

    @Override
    public String md5(String file) throws IOException {
        Process process = Runtime.getRuntime().exec(cmd + " " + file);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String rt= reader.readLine();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("external process failed");
            }
            return rt;
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
