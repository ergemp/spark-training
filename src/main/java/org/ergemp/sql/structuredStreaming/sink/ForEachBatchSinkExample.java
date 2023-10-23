package org.ergemp.sql.structuredStreaming.sink;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;

public class ForEachBatchSinkExample {
    public static void main(String[] args) {

        /*
        streamingDatasetOfString.writeStream().foreachBatch(
                new VoidFunction2<Dataset<String>, Long>() {
                    public void call(Dataset<String> dataset, Long batchId) {
                        // Transform and write batchDF
                    }
                }
        ).start();

        // scala example
        streamingDF.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          batchDF.persist()
          batchDF.write.format(...).save(...)  // location 1
          batchDF.write.format(...).save(...)  // location 2
          batchDF.unpersist()
        }

        Reuse existing batch data sources -
                For many storage systems, there may not be a streaming sink available yet,
                but there may already exist a data writer for batch queries. Using foreachBatch,
                you can use the batch data writers on the output of each micro-batch.

        Write to multiple locations -
                If you want to write the output of a streaming query to multiple locations,
                then you can simply write the output DataFrame/Dataset multiple times.
                However, each attempt to write can cause the output data to be recomputed (including possible re-reading of the input data).
                To avoid recomputations, you should cache the output DataFrame/Dataset,
                write it to multiple locations, and then uncache it. Here is an outline.
        */

    }
}
