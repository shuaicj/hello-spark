package shuaicj.hello.spark.rw.consistency;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
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
    private final MD5er md5er;
    private final String dir;
    private final int num;
    private final int size;

    private volatile boolean stop;

    public ConsistencyChecker(FS fs, MD5er md5er, String dir, int num, int size) {
        this.fs = fs;
        this.md5er = md5er;
        this.dir = dir;
        this.num = num;
        this.size = size;
    }

    public void check() {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> stop = true));

        final JavaSparkContext sc = new JavaSparkContext();
        final List<String> list = list(num);

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
            final boolean rw = Math.random() < 0.5;
            result = sc.parallelize(result, result.size())
                    .map(infos -> rw ? read(infos) : write(infos))
                    .collect();
            if (!result.stream().allMatch(
                    infos -> infos.stream().allMatch(
                            info -> info.getStatus().equals("ok")))) {
                System.out.println(timestamp + (rw ? " read: " : " write: ") + prettyList(result));
                return;
            }
        }
    }

    private List<String> list(int num) {
        List<String> list = new ArrayList<>(num);
        int len = Integer.toString(num).length();
        for (int i = 0; i < num; i++) {
            StringBuilder sb = new StringBuilder();
            String s = Integer.toString(i);
            for (int count = len - s.length(); count > 0; count--) {
                sb.append('0');
            }
            sb.append(s);
            list.add(sb.toString());
        }
        return list;
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
                rt.add(new FileInfo(info.getFile(), info.getSize(), md5er.md5(info.getFile()), "ok"));
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
            return new FileInfo(info.getFile(), info.getSize(), md5er.md5(info.getFile()), "ok");
        } catch (IOException e) {
            return new FileInfo(info.getFile(), info.getSize(), "no md5", e.toString());
        }
    }

    private List<FileInfo> read(List<FileInfo> infos) {
        return infos.stream().map(this::read).collect(Collectors.toList());
    }

    private FileInfo read(FileInfo info) {
        try {
            String actual = md5er.md5(info.getFile());
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
