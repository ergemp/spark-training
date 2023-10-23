package org.ergemp.sql.structuredStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

public class TriggerExamples {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .appName("TriggerExamples")
                .master("local")
                .getOrCreate();

        // Create a streaming DataFrame
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                //.option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "mytopic")
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                ;




        // Default trigger (runs micro-batch as soon as it can)
        //df.writeStream()
        //        .format("console")
        //        .start();

        // ProcessingTime trigger with two-seconds micro-batch interval
        StreamingQuery query = df.writeStream()
                .format("console")
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .start();

        /*
        // One-time trigger (Deprecated, encouraged to use Available-now trigger)
        df.writeStream()
                .format("console")
                .trigger(Trigger.Once())
                .start();

        // Available-now trigger
        df.writeStream()
                .format("console")
                .trigger(Trigger.AvailableNow())
                .start();

        // Continuous trigger with one-second checkpointing interval
        df.writeStream()
                .format("console")
                .trigger(Trigger.Continuous("1 second"))
                .start();
        */

        query.awaitTermination();


    }
}
