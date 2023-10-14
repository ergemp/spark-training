package org.ergemp.sql.structuredStreaming.sink;

public class SinkExamples {
    public static void main(String[] args) {



        // kafka sink
        /*
        writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
            .option("topic", "updates")
            .start()
        */

        // foreach sink
        /*
        writeStream
            .foreach(...)
            .start()
        */

        // console sink (for debugging)
        /*
        writeStream
                .format("console")
                .start()
        */


    }
}
