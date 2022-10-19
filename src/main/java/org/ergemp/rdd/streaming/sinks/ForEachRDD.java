package org.ergemp.rdd.streaming.sinks;

public class ForEachRDD {
    public static void main(String[] args) {
        /* The most generic output operator that applies a function, func, to each RDD generated from the stream.
         * This function should push the data in each RDD to an external system, such as saving the RDD to files,
         * or writing it over the network to a database.
         *
         * Note that the function func is executed in the driver process running the streaming application,
         * and will usually have RDD actions in it that will force the computation of the streaming RDDs.
         * */

        /*
        dstream.foreachRDD(rdd -> {
          Connection connection = createNewConnection(); // executed at the driver
          rdd.foreach(record -> {
            connection.send(record); // executed at the worker
          });
        });

        // or this maybe
        dstream.foreachRDD(rdd -> {
          rdd.foreachPartition(partitionOfRecords -> {
            Connection connection = createNewConnection();
            while (partitionOfRecords.hasNext()) {
              connection.send(partitionOfRecords.next());
            }
            connection.close();
          });
        });

        // correct way to init a connection object
        dstream.foreachRDD(rdd -> {
            rdd.foreachPartition(partitionOfRecords -> {
                // ConnectionPool is a static, lazily initialized pool of connections
                Connection connection = ConnectionPool.getConnection();
                while (partitionOfRecords.hasNext()) {
                    connection.send(partitionOfRecords.next());
                }
                ConnectionPool.returnConnection(connection); // return to the pool for future reuse
            });
        });
        */

        /*
        DStreams are executed lazily by the output operations, just like RDDs are lazily executed by RDD actions.
        Specifically, RDD actions inside the DStream output operations force the processing of the received data.

        Hence, if your application does not have any output operation,
        or has output operations like dstream.foreachRDD() without any RDD action inside them,
        then nothing will get executed. The system will simply receive the data and discard it.
        */
    }
}
