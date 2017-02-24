package shuaicj.hello.spark.word.count;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * Count the words in a file.
 *
 * @author shuaicj 2017/02/23
 */
public class WordCountApp {

    public static void main(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("A file path is required!");
        }

        String path = args[0];
        SparkConf conf = new SparkConf().setAppName("WordCountApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(path);
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());
        Map<String, Integer> counts = words.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (count1, count2) -> count1 + count2)
                .collectAsMap();

        StringBuilder sb = new StringBuilder();
        sb.append("################################\n");
        sb.append("total ");
        sb.append(words.count());
        sb.append("\n\n");
        counts.forEach((word, count) -> {
            sb.append(word);
            sb.append(' ');
            sb.append(count);
            sb.append('\n');
        });
        sb.append("################################");
        System.out.println(sb.toString());

        sc.stop();
    }
}

