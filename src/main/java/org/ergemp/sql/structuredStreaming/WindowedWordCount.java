package org.ergemp.sql.structuredStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.TimeoutException;

public class WindowedWordCount {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException, StreamingQueryException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .appName("WindowedWordCount")
                .master("local")
                .getOrCreate();

        // Read all the csv files written atomically in a directory
        StructType schema = new StructType()
                .add("word", "string")
                .add("timestamp", "string")
                ;

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<Row> lines = spark
                .readStream()
                //.schema(schema)
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .option("includeTimestamp", true)
                .load();

        // Split the lines into words, retaining timestamps
        Dataset<Row> words = lines
                .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
                            List<Tuple2<String, Timestamp>> result = new ArrayList<>();
                            for (String word : t._1.split(" ")) {
                                result.add(new Tuple2<>(word, t._2));
                            }
                            return result.iterator();
                        },
                        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
                ).toDF("word", "timestamp");

        // Group the data by window and word and compute the count of each group
        Dataset<Row> windowedCounts = words.groupBy(
                functions.window(words.col("timestamp"), "10 minutes", "5 minutes"),
                words.col("word")
        ).count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .start();

        query.awaitTermination();

    }
}

/*
ref: https://github.com/apache/spark/blob/v3.5.0/examples/src/main/java/org/apache/spark/examples/sql/streaming/JavaStructuredNetworkWordCountWindowed.java
ref: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

nc -lk 9999

java spark
spark hive
java kafka
java flink
spark streaming
kafka streaming
*/