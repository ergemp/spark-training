package org.ergemp.rdd.streaming.sources;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class FileSource {
    public static void main(String[] args){
        SparkConf conf = new SparkConf()
                .setAppName("FileSource")
                .setMaster("local");

        JavaStreamingContext jssc = new JavaStreamingContext(
                conf,
                new Duration(10000)); //10 secs

        JavaDStream<String> data = jssc.textFileStream("data/fileStreamingFiles" );
        data.print();

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
