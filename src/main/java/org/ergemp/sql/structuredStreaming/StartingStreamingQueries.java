package org.ergemp.sql.structuredStreaming;

public class StartingStreamingQueries {
    public static void main(String[] args) {

        // Once you have defined the final result DataFrame/Dataset,
        // all that is left is for you to start the streaming computation.
        // To do that, you have to use the DataStreamWriter
        // returned through Dataset.writeStream().
        // You will have to specify one or more of the following in this interface.

        /*
        Details of the output sink: Data format, location, etc.

        Output mode: Specify what gets written to the output sink.

        Query name: Optionally, specify a unique name of the query for identification.

        Trigger interval: Optionally, specify the trigger interval. If it is not specified, the system will check for availability of new data as soon as the previous processing has been completed. If a trigger time is missed because the previous processing has not been completed, then the system will trigger processing immediately.

        Checkpoint location: For some output sinks where the end-to-end fault-tolerance can be guaranteed, specify the location where the system will write all the checkpoint information. This should be a directory in an HDFS-compatible fault-tolerant file system. The semantics of checkpointing is discussed in more detail in the next section.
        */

        /*
        * Output modes:
        * append (default)
        * complete
        * update
        * */


    }
}
