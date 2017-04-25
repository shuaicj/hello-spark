package shuaicj.hello.spark.multi.thread;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Submit tasks in multi threads.
 *
 * @author shuaicj 2017/04/25
 */
public class MultiThreadApp {

    public static void main(String[] args) throws Exception {
        final SparkConf conf = new SparkConf().setAppName(MultiThreadApp.class.getSimpleName());
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        Future<Integer> futureOdd = executorService.submit(() -> sc.parallelize(list).map(i -> {
            if (i % 2 != 0) {
                return i;
            }
            return 0;
        }).reduce((a, b) -> a + b));

        Future<Integer> futureEven = executorService.submit(() -> sc.parallelize(list).map(i -> {
            if (i % 2 == 0) {
                return i;
            }
            return 0;
        }).reduce((a, b) -> a + b));

        System.out.println("### odd sum: " + futureOdd.get());
        System.out.println("### even sum: " + futureEven.get());
        System.out.println("### total sum: " + (futureOdd.get() + futureEven.get()));

        executorService.shutdown();
        sc.stop();
    }
}
