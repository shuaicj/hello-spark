package shuaicj.hello.spark.parallelize;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Count the words in a file.
 *
 * @author shuaicj 2017/04/06
 */
public class ParallelizeApp {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(ParallelizeApp.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.parallelize(Arrays.asList("10", "20", "30", "40", "50", "60"));
        System.out.println(sc.appName() + " ################# " + lines.count());

        sc.stop();
    }
}

