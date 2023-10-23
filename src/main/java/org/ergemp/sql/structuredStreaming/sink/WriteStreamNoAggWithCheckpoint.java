package org.ergemp.sql.structuredStreaming.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.concurrent.TimeoutException;

public class WriteStreamNoAggWithCheckpoint {
    public static void main(String[] args) throws TimeoutException {

        /*
        // ========== DF with no aggregations ==========
        Dataset<Row> noAggDF = deviceDataDf.select("device").where("signal > 10");

        // Print new data to console
        noAggDF
                .writeStream()
                .format("console")
                .start();

        // Write new data to Parquet files
        noAggDF
                .writeStream()
                .format("parquet")
                .option("checkpointLocation", "path/to/checkpoint/dir")
                .option("path", "path/to/destination/dir")
                .start();

        */

    }
}
