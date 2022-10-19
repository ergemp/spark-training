package org.ergemp.rdd.streaming.windowing;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class ReduceByKeyAndWindowExample {
    public static void main(String[] args) {
        Logger log = Logger.getRootLogger();
        log.setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("ReduceByKeyAndWindowExample")
                .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));
        jssc.checkpoint("checkpoint/ReduceByKeyAndWindowExample");

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 19999);

        JavaPairDStream<String, Integer> wordCounts = lines
                .flatMap(line -> Arrays.asList(line.replaceAll("\\s+" , " ").split(" ")).iterator())
                .mapToPair(line -> new Tuple2<String,Integer>(line,1))
                ;
        JavaPairDStream<String, Integer> windowedWordCounts = wordCounts.reduceByKeyAndWindow((i1, i2) -> i1 + i2, Durations.seconds(30), Durations.seconds(10));
        windowedWordCounts.print();

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

/*
reduceByKeyAndWindow(func, windowLength, slideInterval, [numTasks])

When called on a DStream of (K, V) pairs, returns a new DStream of (K, V) pairs
where the values for each key are aggregated using the given reduce function func over batches in a sliding window.

Note: By default, this uses Spark's default number of parallel tasks
(2 for local mode, and in cluster mode the number is determined by the config property spark.default.parallelism)
to do the grouping. You can pass an optional numTasks argument to set a different number of tasks.
*/

/*
* reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks])

* A more efficient version of the above reduceByKeyAndWindow()
* where the reduce value of each window is calculated incrementally using the reduce values of the previous window.
*
* This is done by reducing the new data that enters the sliding window,
* and “inverse reducing” the old data that leaves the window.
*
* An example would be that of “adding” and “subtracting” counts of keys as the window slides.
* However, it is applicable only to “invertible reduce functions”,
* that is, those reduce functions which have a corresponding “inverse reduce” function
* (taken as parameter invFunc).
*
* Like in reduceByKeyAndWindow, the number of reduce tasks is configurable through an optional argument.
* Note that checkpointing must be enabled for using this operation.
* */
