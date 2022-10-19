package org.ergemp.rdd.streaming.actions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class ForEachRDDExample1 {
    public static void main(String[] args){
        Logger log = Logger.getRootLogger();
        log.setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("ForEachRDDExample1")
                .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));
        JavaDStream<String> lines = jssc.socketTextStream("localhost", 19999);


        lines.foreachRDD(rdd -> {
                    System.out.println("Size of the RDD: " + rdd.collect().size());
                }
                );

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}

/*

foreachRDD(func)

The most generic output operator that applies a function, func,
to each RDD generated from the stream.
This function should push the data in each RDD to an external system, such as saving the RDD to files,
or writing it over the network to a database. Note that the function func is executed in the driver process
running the streaming application, and will usually have RDD actions in it that will force the computation
of the streaming RDDs.
*/