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
                .appName("DropDuplicatesExample")
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

        /*
        You can deduplicate records in data streams using a unique identifier in the events.
        This is exactly same as deduplication on static using a unique identifier column.
        The query will store the necessary amount of data from previous records such that it can filter duplicate records.
        Similar to aggregations, you can use deduplication with or without watermarking.
        */

        /*
        With watermark - If there is an upper bound on how late a duplicate record may arrive,
        then you can define a watermark on an event time column and deduplicate using both the guid and the event time columns.
        The query will use the watermark to remove old state data from past records that are not expected to get any duplicates any more.
        This bounds the amount of the state the query has to maintain.

        Without watermark - Since there are no bounds on when a duplicate record may arrive,
        the query stores the data from all the past records as state.
        */


    }
}
