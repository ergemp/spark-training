package org.ergemp.rdd.streaming.streamingContext;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class CreateJavaStreamingContextFromJavaContext {
    public static void main(String[] args){
        SparkConf conf = new SparkConf()
                .setAppName("CreateJavaStreamingContextFromJavaContext")
                .setMaster("local")
                .set("spark.driver.allowMultipleContext", "true");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(10));
    }
}
