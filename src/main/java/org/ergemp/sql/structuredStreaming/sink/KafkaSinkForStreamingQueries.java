package org.ergemp.sql.structuredStreaming.sink;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.concurrent.TimeoutException;

public class KafkaSinkForStreamingQueries {
    public static void main(String[] args) throws TimeoutException {

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

        // Write key-value data from a DataFrame to a specific Kafka topic specified in an option
        StreamingQuery ds = df
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .option("topic", "topic1")
                .start();

        // Write key-value data from a DataFrame to Kafka using a topic specified in the data
        StreamingQuery ds2 = df
                .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .start();

    }
}
