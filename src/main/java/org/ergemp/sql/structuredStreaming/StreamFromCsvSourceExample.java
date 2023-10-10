package org.ergemp.sql.structuredStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class StreamFromCsvSourceExample {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .appName("StreamFromCsvSourceExample")
                .master("local")
                .getOrCreate();

        // Read text from socket
        Dataset<Row> socketDF = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        socketDF.isStreaming();    // Returns True for DataFrames that have streaming sources
        socketDF.printSchema();

        // Read all the csv files written atomically in a directory
        StructType userSchema = new StructType()
                .add("name", "string")
                .add("age", "integer");

        Dataset<Row> csvDF = spark
                .readStream()
                .option("sep", ";")
                .schema(userSchema)      // Specify schema of the csv files
                .csv("data/streaming/StreamFromCsvSourceExample/");    // Equivalent to format("csv").load("/path/to/directory")

        // Start running the query that prints the running counts to the console
        StreamingQuery query = csvDF.writeStream()
                //.outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();

    }
}
