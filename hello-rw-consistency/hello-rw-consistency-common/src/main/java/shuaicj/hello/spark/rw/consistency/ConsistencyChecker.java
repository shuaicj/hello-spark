package shuaicj.hello.spark.rw.consistency;

import org.apache.spark.api.java.JavaSparkContext;

import javax.xml.bind.DatatypeConverter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

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

        final long timestamp = System.currentTimeMillis();

        // create files
        List<List<FileInfo>> result = sc.parallelize(list, list.size())
                .map(i -> create(dir + "/" + timestamp + "-" + i))
                .collect();
        if (!result.stream().allMatch(
                infos -> infos.stream().allMatch(
                        info -> info.getStatus().equals("ok")))) {
            System.out.println(timestamp + " create: " + prettyList(result));
            return;
        }

        // read or write randomly
        while (!stop) {
            result = sc.parallelize(result, result.size())
                    .map(infos -> Math.random() < 0.5 ? read(infos) : write(infos))
                    .collect();
            if (!result.stream().allMatch(
                    infos -> infos.stream().allMatch(
                            info -> info.getStatus().equals("ok")))) {
                System.out.println(timestamp + " rw: " + prettyList(result));
                return;
            }
        }
    }

    /**
     * Write two files with the same content for comparison.
     * @param prefix two files will be named as ${prefix}-0 and ${prefix}-1
     * @return file info of two
     */
    private List<FileInfo> create(String prefix) {
        return write(Arrays.asList(
                new FileInfo(prefix + "-0", size),
                new FileInfo(prefix + "-1", size)
        ));
    }

    private List<FileInfo> write(List<FileInfo> infos) {
        List<FileInfo> rt = new ArrayList<>(infos.size());
        byte[] buf = new byte[size];
        Random r = new Random();
        r.nextBytes(buf);
        for (FileInfo info : infos) {
            try {
                fs.write(info.getFile(), buf);
                rt.add(new FileInfo(info.getFile(), info.getSize(), md5(info.getFile()), "ok"));
            } catch (IOException e) {
                rt.add(new FileInfo(info.getFile(), info.getSize(), "no md5", e.toString()));
            }
        }
        String md5 = rt.get(0).getMd5();
        if (rt.stream().allMatch(info -> info.getStatus().equals("ok") && info.getMd5().equals(md5))) {
            return rt;
        }
        return rt.stream()
                .map(info -> new FileInfo(info.getFile(), info.getSize(), info.getMd5(), "not same"))
                .collect(Collectors.toList());
    }

    private FileInfo write(FileInfo info) {
        byte[] buf = new byte[size];
        Random r = new Random();
        r.nextBytes(buf);
        try {
            fs.write(info.getFile(), buf);
            return new FileInfo(info.getFile(), info.getSize(), md5(info.getFile()), "ok");
        } catch (IOException e) {
            return new FileInfo(info.getFile(), info.getSize(), "no md5", e.toString());
        }
    }

    private List<FileInfo> read(List<FileInfo> infos) {
        return infos.stream().map(this::read).collect(Collectors.toList());
    }

    private FileInfo read(FileInfo info) {
        try {
            String actual = md5(info.getFile());
            return info.getMd5().equals(actual) ?
                    new FileInfo(info.getFile(), info.getSize(), info.getMd5(), "ok") :
                    new FileInfo(info.getFile(), info.getSize(), info.getMd5(), "actual:" + actual);
        } catch (IOException e) {
            return new FileInfo(info.getFile(), info.getSize(), info.getMd5(), e.toString());
        }
    }

    // private String delete(FileInfo info) {
    //     try {
    //         return fs.delete(info.getFile()) ? "ok" : "failed";
    //     } catch (Exception e) {
    //         return e.toString();
    //     }
    // }
    //
    // private String exists(FileInfo info) {
    //     try {
    //         return fs.exists(info.getFile()) ? "ok" : "failed";
    //     } catch (Exception e) {
    //         return e.toString();
    //     }
    // }

    private String md5(String file) throws IOException {
        // return DigestUtils.md5DigestAsHex(fs.read(file));
        try (InputStream in = new FileInputStream(file)) {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] buf = new byte[4096];
            for (int len = in.read(buf); len != -1; len = in.read(buf)) {
                md.update(buf, 0, len);
                Thread.sleep(100);
            }
            return DatatypeConverter.printHexBinary(md.digest());
        } catch (InterruptedException | NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }

    // private String prettyMap(Map<String, Long> map) {
    //     StringBuilder sb = new StringBuilder();
    //     new TreeMap<>(map).forEach((k, v) -> sb.append(k).append(" n:").append(v).append('\n'));
    //     return sb.toString();
    // }

    private String prettyList(List<List<FileInfo>> result) {
        StringBuilder sb = new StringBuilder();
        sb.append('\n');
        for (List<FileInfo> infos : result) {
            for (FileInfo info : infos) {
                sb.append(info.getFile()).append(' ')
                        .append(info.getMd5()).append(' ')
                        .append(info.getStatus()).append('\n');
            }
        }
        return sb.toString();
    }
}
