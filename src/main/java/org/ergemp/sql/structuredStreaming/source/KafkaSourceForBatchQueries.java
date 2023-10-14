package org.ergemp.sql.structuredStreaming.source;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KafkaSourceForBatchQueries {
    public static void main(String[] args) {

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



    }
}
