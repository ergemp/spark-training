package org.ergemp.sql.structuredStreaming.sink;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.concurrent.TimeoutException;

public class KafkaSinkOptions {
    public static void main(String[] args) throws TimeoutException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .appName("KafkaSinkOptions")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                //.option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "mytopic")
                .load();


        StreamingQuery ds = df
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .option("topic", "topic1")

                .option("group.id", "KafkaSinkOptions-v1")

                .option("key.deserializer", "StringDeserializer")
                .option("value.deserializer", "StringDeserializer")

                .option("key.serializer", "StringSerializer")
                .option("value.serializer", "StringSerializer")

                .start();

    }
}
