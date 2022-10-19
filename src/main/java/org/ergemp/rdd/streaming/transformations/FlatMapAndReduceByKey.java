package org.ergemp.rdd.streaming.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class FlatMapAndReduceByKey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ReduceByKey")
                .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 19999);
        JavaPairDStream<String,Integer> wordCounts = lines
                .flatMap(line -> Arrays.asList(line.replaceAll("\\s+"," ").split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a,b) -> a+b)
                ;

        wordCounts.print();

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

/*
reduceByKey(func, [numTasks])

When called on a DStream of (K, V) pairs, return a new DStream of (K, V) pairs where
the values for each key are aggregated using the given reduce function.

Note: By default, this uses Spark's default number of parallel tasks
(2 for local mode, and in cluster mode the number is determined by the config property spark.default.parallelism)
to do the grouping. You can pass an optional numTasks argument to set a different number of tasks.
*/
