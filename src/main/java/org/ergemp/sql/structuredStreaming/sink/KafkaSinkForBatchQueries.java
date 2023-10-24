package org.ergemp.sql.structuredStreaming.sink;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KafkaSinkForBatchQueries {
    public static void main(String[] args) {

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
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .write()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .option("topic", "topic1")
                .save();

        // Write key-value data from a DataFrame to Kafka using a topic specified in the data
        df.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
                .write()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .save();


    }
}
