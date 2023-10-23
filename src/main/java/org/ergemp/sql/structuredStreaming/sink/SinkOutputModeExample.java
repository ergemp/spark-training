package org.ergemp.sql.structuredStreaming.sink;

public class SinkOutputModeExample {
    public static void main(String[] args) {

        /*
        ref: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

        Append mode (default) -

        This is the default mode, where only the new rows added to the Result Table since the last trigger will be outputted to the sink.
        This is supported for only those queries where rows added to the Result Table is never going to change.
        Hence, this mode guarantees that each row will be output only once (assuming fault-tolerant sink).
        For example, queries with only select, where, map, flatMap, filter, join, etc. will support Append mode.

        Complete mode -

        The whole Result Table will be outputted to the sink after every trigger.
        This is supported for aggregation queries.

        Update mode -

        (Available since Spark 2.1.1) Only the rows in the Result Table that were updated since the last trigger
        will be outputted to the sink. More information to be added in future releases.
        */



    }
}
