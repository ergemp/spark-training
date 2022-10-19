package org.ergemp.rdd.streaming.windowing;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class CountByValueAndWindowExample {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("CountByValueAndWindowExample")
                .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));
        jssc.checkpoint("checkpoint/CountByValueAndWindowExample");

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 19999);

        JavaPairDStream<String, Long> windowedWordCounts = lines.countByValueAndWindow(
                Durations.seconds(30), Durations.seconds(10));
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
countByValueAndWindow(windowLength, slideInterval, [numTasks])

When called on a DStream of (K, V) pairs, returns a new DStream of (K, Long) pairs
where the value of each key is its frequency within a sliding window.
Like in reduceByKeyAndWindow, the number of reduce tasks is configurable through an optional argument.


window length - The duration of the window.
sliding interval - The interval at which the window operation is performed.

*/