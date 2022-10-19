package org.ergemp.rdd.streaming.sources;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SocketSource2 {
    public static void main(String[] args){
        SparkConf conf = new SparkConf()
                .setAppName("SocketSource2")
                .setMaster("local");

        JavaStreamingContext jssc = new JavaStreamingContext(
                conf,
                new Duration(10000)); //10 secs

        JavaDStream<String> lines = jssc.socketTextStream(
                "localhost",
                19999);
        lines.print();

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
