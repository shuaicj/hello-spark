package shuaicj.hello.spark.rw.consistency;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

            Map<String, Long> writeResult = sc.parallelize(list, list.size())
                    .map(i -> write(dir + "/" + timestamp + "-" + i, String.valueOf(i), size))
                    .countByValue();
            System.out.println(timestamp + " write: " + writeResult);
            if (!writeResult.containsKey("ok") || writeResult.get("ok") != list.size()) {
                break;
            }

            Map<String, Long> readwResult = sc.parallelize(list, list.size())
                    .map(i -> read(dir + "/" + timestamp + "-" + i, String.valueOf(i), size))
                    .countByValue();
            System.out.println(timestamp + " readw: " + readwResult);
            if (!readwResult.containsKey("ok") || readwResult.get("ok") != list.size()) {
                break;
            }

            Map<String, Long> deleteResult = sc.parallelize(list, list.size())
                    .map(i -> delete(dir + "/" + timestamp + "-" + i))
                    .countByValue();
            System.out.println(timestamp + " delete: " + deleteResult);
            if (!deleteResult.containsKey("ok") || deleteResult.get("ok") != list.size()) {
                break;
            }

            Map<String, Long> readdResult = sc.parallelize(list, list.size())
                    .map(i -> read(dir + "/" + timestamp + "-" + i, String.valueOf(i), size))
                    .countByValue();
            if (readdResult.containsKey("ok")) {
                break;
            }
        }
    }

    private static String write(String file, String text, int size) {
        try (Writer writer = new FileWriter(file)) {
            int count = size / text.length();
            for (int i = 0; i < count; i++) {
                writer.write(text);
            }
        } catch (IOException e) {
            return e.toString();
        }
        return "ok";
    }

    private static String read(String file, String text, int size) {
        try (Reader reader = new FileReader(file)) {
            int count = size / text.length();
            char[] chars = text.toCharArray();
            char[] buf = new char[chars.length];
            for (int i = 0; i < count; i++) {
                int num = reader.read(buf);
                if (num != chars.length || !Arrays.equals(chars, buf)) {
                    return "content not same";
                }
            }
        } catch (IOException e) {
            return e.toString();
        }
        return "ok";
    }

    private static String delete(String file) {
        try {
            return new File(file).delete() ? "ok" : "failed";
        } catch (SecurityException e) {
            return e.toString();
        }
    }
}
