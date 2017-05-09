package shuaicj.hello.spark.rw.consistency;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

/**
 * File info.
 *
 * @author shuaicj 2017/05/09
 */
@SuppressWarnings("serial")
@Getter
@AllArgsConstructor
public class FileInfo implements Serializable {
    private final String status, file, md5;
}

