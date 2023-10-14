package org.ergemp.sql.structuredStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DropDuplicatesExample {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .appName("WindowedWordCount")
                .master("local")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<Row> lines = spark
                .readStream()
                //.schema(schema)
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .option("includeTimestamp", true)
                .load();

        // Without watermark using guid column
        lines.dropDuplicates("guid");

        // With watermark using guid and eventTime columns
        lines
                .withWatermark("eventTime", "10 seconds")
                .dropDuplicates("guid", "eventTime");

    }
}
