package shuaicj.hello.spark.rw.consistency;

import lombok.Getter;

import java.io.Serializable;

/**
 * File info.
 *
 * @author shuaicj 2017/05/09
 */
@SuppressWarnings("serial")
@Getter
public class FileInfo implements Serializable {

    private final String file;
    private final int size;
    private final String md5;
    private final String status;

    public FileInfo(String file, int size) {
        this(file, size, "no md5", "no status");
    }

    public FileInfo(String file, int size, String md5, String status) {
        this.file = file;
        this.size = size;
        this.md5 = md5;
        this.status = status;
    }
}

