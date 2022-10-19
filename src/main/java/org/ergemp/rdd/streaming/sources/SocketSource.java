package org.ergemp.rdd.streaming.sources;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SocketSource {
    public static void main(String[] args){
        SparkConf conf = new SparkConf()
                .setAppName("SocketSource")
                .setMaster("local");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 19999);
        JavaDStream<String> errLines = lines.filter(line -> line.contains("error"));

        errLines.print();

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

/*
* usage:
# TERMINAL 1:
# Running Netcat
$ nc -lk 9999
hello world
* */
