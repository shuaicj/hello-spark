package shuaicj.hello.spark.rw.consistency;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.util.DigestUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * Check read/write consistency.
 *
 * @author shuaicj 2017/05/09
 */
@SuppressWarnings("serial")
public class ConsistencyChecker implements Serializable {

    private final FS fs;
    private final String dir;
    private final int num;
    private final int size;

    private volatile boolean stop;

    public ConsistencyChecker(FS fs, String dir, int num, int size) {
        this.fs = fs;
        this.dir = dir;
        this.num = num;
        this.size = size;
    }

    public void check() {

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
                        .map(this::read)
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
                    .map(this::delete)
                    .countByValue();
            if (!deleteResult.containsKey("ok") || deleteResult.get("ok") != writeResult.size()) {
                System.out.println(timestamp + " delete: " + deleteResult);
                break;
            }

            // check existence
            Map<String, Long> existResult = sc.parallelize(writeResult, writeResult.size())
                    .map(this::exists)
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
    private Iterator<FileInfo> write(String prefix, int size) {
        byte[] buf = new byte[size];
        Random r = new Random();
        r.nextBytes(buf);
        String file0 = prefix + "-0";
        String file1 = prefix + "-1";
        try {
            fs.write(file0, buf);
            fs.write(file1, buf);
        } catch (IOException e) {
            return Arrays.asList(new FileInfo(e.toString(), file0, ""), new FileInfo(e.toString(), file1, "")).iterator();
        }

        String md50 = md5(file0);
        String md51 = md5(file1);
        String status = md50.equals(md51) ? "ok" : "copy not same";
        return Arrays.asList(new FileInfo(status, file0, md50), new FileInfo(status, file1, md51)).iterator();
    }

    private String read(FileInfo info) {
        String actual = md5(info.getFile());
        return info.getMd5().equals(actual) ? "ok" :
                "file:" + info.getFile() + " expect:" + info.getMd5() + " actual:" + actual;
    }

    private String delete(FileInfo info) {
        try {
            return fs.delete(info.getFile()) ? "ok" : "failed";
        } catch (Exception e) {
            return e.toString();
        }
    }

    private String exists(FileInfo info) {
        try {
            return fs.exists(info.getFile()) ? "ok" : "failed";
        } catch (Exception e) {
            return e.toString();
        }
    }

    private String md5(String file) {
        try {
            return DigestUtils.md5DigestAsHex(fs.read(file));
        } catch (Exception e) {
            return e.toString();
        }
    }

    private String prettyMap(Map<String, Long> map) {
        StringBuilder sb = new StringBuilder();
        new TreeMap<>(map).forEach((k, v) -> sb.append(k).append(" n:").append(v).append('\n'));
        return sb.toString();
    }
}
