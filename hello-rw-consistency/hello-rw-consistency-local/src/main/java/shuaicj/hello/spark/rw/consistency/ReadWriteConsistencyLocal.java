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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

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

            List<FileInfo> writeResult = sc.parallelize(list, list.size())
                    .flatMap(i -> write(dir + "/" + timestamp + "-" + i, size))
                    .collect();
            if (!writeResult.stream().allMatch(info -> info.getStatus().equals("ok"))) {
                System.out.println(timestamp + " write: " + writeResult);
                break;
            }

            Map<String, Long> readResult = sc.parallelize(writeResult, writeResult.size())
                    .map(ReadWriteConsistencyLocal::read)
                    .countByValue();
            if (!readResult.containsKey("ok") || readResult.get("ok") != writeResult.size()) {
                System.out.println(timestamp + " read: " + readResult);
                break;
            }

            Map<String, Long> deleteResult = sc.parallelize(writeResult, writeResult.size())
                    .map(ReadWriteConsistencyLocal::delete)
                    .countByValue();
            if (!deleteResult.containsKey("ok") || deleteResult.get("ok") != writeResult.size()) {
                System.out.println(timestamp + " delete: " + deleteResult);
                break;
            }

            Map<String, Long> existResult = sc.parallelize(writeResult, writeResult.size())
                    .map(ReadWriteConsistencyLocal::exists)
                    .countByValue();
            if (existResult.containsKey("ok")) {
                System.out.println(timestamp + " exists: " + existResult);
                break;
            }
        }
    }

    private static Iterator<FileInfo> write(String prefix, int size) {
        Random r = new Random();
        byte[] buf = new byte[1024];
        List<FileInfo> list = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            String file = prefix + "-" + i;
            try (OutputStream out = new FileOutputStream(file)) {
                for (int count = size; count > 0; count -= buf.length) {
                    r.nextBytes(buf);
                    out.write(buf, 0, count < buf.length ? count : buf.length);
                }
                out.flush();
            } catch (IOException e) {
                list.add(new FileInfo(e.toString(), file, ""));
                continue;
            }
            list.add(new FileInfo("ok", file, md5(file)));
        }
        return list.iterator();
    }

    private static String read(FileInfo info) {
        String actual = md5(info.getFile());
        return info.getMd5().equals(actual) ? "ok" : "actual:" + actual + " " + info;
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

    @SuppressWarnings("serial")
    @Getter
    @AllArgsConstructor
    @ToString(exclude = "status")
    public static class FileInfo implements Serializable {
        private final String status, file, md5;
    }
}
