package org.ergemp.sql.structuredStreaming.sink;

import java.util.concurrent.TimeoutException;

public class WriteStreamMemorySinkWithAgg {
    public static void main(String[] args) throws TimeoutException {

        /*
        // ========== DF with aggregation ==========
        Dataset<Row> aggDF = df.groupBy("device").count();

        // Print updated aggregations to console
        aggDF
                .writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        // Have all the aggregates in an in-memory table
        aggDF
                .writeStream()
                .queryName("aggregates")    // this query name will be the table name
                .outputMode("complete")
                .format("memory")
                .start();

        spark.sql("select * from aggregates").show();   // interactively query in-memory table
        */

    }
}
