package org.ergemp.sql.structuredStreaming.sink;

import org.apache.spark.sql.ForeachWriter;

public class ForEachSinkExample {
    public static void main(String[] args) {

        // foreach sink
        /*
        writeStream
            .foreach(...)
            .start()
        */


        /*
        streamingDatasetOfString.writeStream().foreach(
                new ForeachWriter<String>() {

                    @Override public boolean open(long partitionId, long version) {
                        // Open connection
                    }

                    @Override public void process(String record) {
                        // Write string to connection
                    }

                    @Override public void close(Throwable errorOrNull) {
                        // Close the connection
                    }
                }
        ).start();
        */

    }
}
