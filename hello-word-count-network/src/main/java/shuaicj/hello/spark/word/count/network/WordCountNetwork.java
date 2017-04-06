package shuaicj.hello.spark.word.count.network;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Count the words from network specified by host and port.
 *
 * @author shuaicj 2017/02/27
 */
public class WordCountNetwork {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("specify the host and ip");
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        SparkConf conf = new SparkConf().setAppName(WordCountNetwork.class.getSimpleName());
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, port);
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> counts = words.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((count1, count2) -> count1 + count2);
        counts.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
