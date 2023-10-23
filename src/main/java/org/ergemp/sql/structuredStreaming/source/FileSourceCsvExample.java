package org.ergemp.sql.structuredStreaming.source;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class FileSourceCsvExample {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        /*
        Reads files written in a directory as a stream of data.
        Files will be processed in the order of file modification time.
        If latestFirst is set, order will be reversed.
        Supported file formats are text, CSV, JSON, ORC, Parquet.
        */

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .appName("FileSourceCsvExample")
                .master("local")
                .getOrCreate();

        /*
        Schema must be specified when creating a streaming source DataFrame. If some
        files already exist in the directory, then depending on the file format you
        may be able to create a static DataFrame on that directory with
        'spark.read.load(directory)' and infer schema from it.
        */

        StructType userSchema = new StructType()
                .add("name", "string")
                .add("age", "integer");

        // Read text from socket
        Dataset<Row> df = spark
                .readStream()
                .schema(userSchema)
                .option("sep", ";")
                //.schema(userSchema)      // Specify schema of the csv files
                .csv("data/streaming/FileSourceCsvExample");    // Equivalent to format("csv").load("/path/to/directory")

        // Start running the query that prints the running counts to the console
        StreamingQuery query = df.writeStream()
                //.outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
