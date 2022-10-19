package org.ergemp.rdd.streaming.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class RememberOldStreams {
    public static void main(String[] args) {

        Logger log = Logger.getRootLogger();
        log.setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("UsingSparkSQLwithForEachRDD")
                .setMaster("local[*]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));

        //remember old streams for 5 minutes
        jssc.remember(new Duration(300000));

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 19999);

    }
}
