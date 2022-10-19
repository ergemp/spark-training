package org.ergemp.rdd.streaming.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class FilterAndMap {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("FilterAndMap")
                .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 19999);
        JavaDStream<String> errLines = lines
                .filter(line -> line.contains("error"))
                .map(line-> line.replaceAll("error", "hata"))
                ;

        errLines.print();

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
