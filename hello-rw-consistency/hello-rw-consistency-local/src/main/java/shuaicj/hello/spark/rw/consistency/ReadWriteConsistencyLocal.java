package shuaicj.hello.spark.rw.consistency;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.util.DigestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * Verify r/w consistency on Ceph local storage.
 * The distributed file system - Ceph, is mount as a local path on each spark slave.
 *
 * @author shuaicj 2017/05/04
 */
public class ReadWriteConsistencyLocal {

    private static volatile boolean stop = false;

    public static void main(String[] args) throws Exception {
        if (args.length != 6
                || !args[0].equals("--dir")
                || !args[2].equals("--num") || Integer.parseInt(args[3]) <= 0
                || !args[4].equals("--size") || Integer.parseInt(args[5]) <= 0) {
            throw new IllegalArgumentException("dir and num required");
        }

        final String dir = args[1];
        final int num = Integer.parseInt(args[3]);
        final int size = Integer.parseInt(args[5]);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> stop = true));

        final JavaSparkContext sc = new JavaSparkContext();
        final List<Integer> list = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            list.add(i);
        }

        while (!stop) {
            final long timestamp = System.currentTimeMillis();

            // write files
            List<FileInfo> writeResult = sc.parallelize(list, list.size())
                    .flatMap(i -> write(dir + "/" + timestamp + "-" + i, size))
                    .collect();
            if (!writeResult.stream().allMatch(info -> info.getStatus().equals("ok"))) {
                System.out.println(timestamp + " write: " + writeResult);
                break;
            }

            // read for three times
            boolean readFailed = false;
            List<Map<String, Long>> readResults = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                Map<String, Long> readResult = sc.parallelize(writeResult, writeResult.size())
                        .map(ReadWriteConsistencyLocal::read)
                        .countByValue();
                if (!readResult.containsKey("ok") || readResult.get("ok") != writeResult.size()) {
                    readFailed = true;
                }
                readResults.add(readResult);
            }
            if (readFailed) {
                for (Map<String, Long> readResult : readResults) {
                    System.out.println(timestamp + " read:\n" + prettyMap(readResult));
                }
                break;
            }

            // delete files
            Map<String, Long> deleteResult = sc.parallelize(writeResult, writeResult.size())
                    .map(ReadWriteConsistencyLocal::delete)
                    .countByValue();
            if (!deleteResult.containsKey("ok") || deleteResult.get("ok") != writeResult.size()) {
                System.out.println(timestamp + " delete: " + deleteResult);
                break;
            }

            // check existence
            Map<String, Long> existResult = sc.parallelize(writeResult, writeResult.size())
                    .map(ReadWriteConsistencyLocal::exists)
                    .countByValue();
            if (existResult.containsKey("ok")) {
                System.out.println(timestamp + " exists: " + existResult);
                break;
            }
        }
    }

    /**
     * Write two files with the same content for comparison.
     * @param prefix two files will be named as ${prefix}-0 and ${prefix}-1
     * @param size file size
     * @return file info of two
     */
    private static Iterator<FileInfo> write(String prefix, int size) {
        Random r = new Random();
        byte[] buf = new byte[1024];
        String file0 = prefix + "-0";
        String file1 = prefix + "-1";
        try (OutputStream out0 = new FileOutputStream(file0)) {
            try (OutputStream out1 = new FileOutputStream(file1)) {
                for (int count = size; count > 0; count -= buf.length) {
                    r.nextBytes(buf);
                    out0.write(buf, 0, count < buf.length ? count : buf.length);
                    out1.write(buf, 0, count < buf.length ? count : buf.length);
                }
                out0.flush();
                out1.flush();
            }
        } catch (IOException e) {
            return Arrays.asList(new FileInfo(e.toString(), file0, ""), new FileInfo(e.toString(), file1, "")).iterator();
        }

        String md50 = md5(file0);
        String md51 = md5(file1);
        String status = md50.equals(md51) ? "ok" : "copy not same";
        return Arrays.asList(new FileInfo(status, file0, md50), new FileInfo(status, file1, md51)).iterator();
    }

    private static String read(FileInfo info) {
        String actual = md5(info.getFile());
        return info.getMd5().equals(actual) ? "ok" :
                "file:" + info.getFile() + " expect:" + info.getMd5() + " actual:" + actual;
    }

    private static String delete(FileInfo info) {
        try {
            return new File(info.getFile()).delete() ? "ok" : "failed";
        } catch (SecurityException e) {
            return e.toString();
        }
    }

    private static String exists(FileInfo info) {
        return new File(info.getFile()).exists() ? "ok" : "failed";
    }

    private static String md5(String file) {
        try (InputStream in = new FileInputStream(file)) {
            return DigestUtils.md5DigestAsHex(in);
        } catch (Exception e) {
            return e.toString();
        }
    }

    private static String prettyMap(Map<String, Long> map) {
        StringBuilder sb = new StringBuilder();
        new TreeMap<>(map).forEach((k, v) -> sb.append(k).append(" n:").append(v).append('\n'));
        return sb.toString();
    }

    @SuppressWarnings("serial")
    @Getter
    @AllArgsConstructor
    @ToString(exclude = "status")
    public static class FileInfo implements Serializable {
        private final String status, file, md5;
    }
}
