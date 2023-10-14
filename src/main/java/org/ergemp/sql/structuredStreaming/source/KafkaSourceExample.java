package org.ergemp.sql.structuredStreaming.source;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class KafkaSourceExample {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        // ref: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .appName("KafkaSourceExample")
                .master("local")
                .getOrCreate();

        // Subscribe to 1 topic
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                //.option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "mytopic")
                .load();

        /*
        // Subscribe to multiple topics
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .option("subscribe", "topic1,topic2")
                .load();
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        // Subscribe to a pattern
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .option("subscribePattern", "topic.*")
                .load();
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        */

        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        StreamingQuery query = df
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .start();

        query.awaitTermination();

    }
}
