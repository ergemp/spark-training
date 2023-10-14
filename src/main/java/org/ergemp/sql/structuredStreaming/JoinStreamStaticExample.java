package org.ergemp.sql.structuredStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JoinStreamStaticExample {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .appName("JoinStreamStaticExample")
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

        /*
        Dataset<Row> staticDf = spark.read(). ...;
        Dataset<Row> streamingDf = spark.readStream(). ...;

        streamingDf.join(staticDf, "type");  // inner equi-join with a static DF
        streamingDf.join(staticDf, "type", "left_outer");  // left outer join with a static DF
        */

    }
}
