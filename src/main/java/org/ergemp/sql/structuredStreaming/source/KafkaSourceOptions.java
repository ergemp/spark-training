package org.ergemp.sql.structuredStreaming.source;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KafkaSourceOptions {
    public static void main(String[] args) {


        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .appName("KafkaSourceExample")
                .master("local")
                .getOrCreate();

        // Subscribe to 1 topic
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092") // A comma-separated list of host:port
                .option("subscribe", "mytopic") // A comma-separated list of topics
                .option("subscribePattern", "mytopic*") // Java regex string
                .option("assign", "mytopic") // json string {"topicA":[0,1],"topicB":[2,4]}

                .option("startingTimestamp", "1000") // timestamp string e.g. "1000"
                                                     // default: none (next preference is startingOffsetsByTimestamp)

                .option("startingOffsetsByTimestamp", "{\"topicA\":{\"0\": 1000, \"1\": 1000}}")     // json string """ {"topicA":{"0": 1000, "1": 1000}, "topicB": {"0": 2000, "1": 2000}} """
                                                                                                     // default: none (next preference is startingOffsets)

                .option("startingOffsets", "latest")   // "earliest", "latest" (streaming only), or json string """ {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}} """
                                                        // default: "latest" for streaming, "earliest" for batch

                .option("endingTimestamp", "1000")  // timestamp string e.g. "1000"
                                                    // default: none (next preference is endingOffsetsByTimestamp)
                                                    // batch only

                .option("endingOffsetsByTimestamp", "{\"topicA\":{\"0\": 1000, \"1\": 1000}")   // json string """ {"topicA":{"0": 1000, "1": 1000}, "topicB": {"0": 2000, "1": 2000}} """
                                                                                                // default: none (next preference is endingOffsets)
                                                                                                // batch only

                .option("endingOffsets", "1000")    // 	latest or json string {"topicA":{"0":23,"1":-1},"topicB":{"0":-1}}
                                                    // default: latest
                                                    // // batch only

                .option("kafkaConsumer.pollTimeoutMs", 120000)

                .option("fetchOffset.numRetries", 3)
                .option("fetchOffset.retryIntervalMs", 10)

                .option("maxOffsetsPerTrigger", 10)  // default: None
                                                     // streaming query only
                                                     // Rate limit on maximum number of offsets processed per trigger interval.
                                                     // The specified total number of offsets will be proportionally split across topicPartitions of different volume.


                .option("minOffsetsPerTrigger", 10)  // default: None
                                                     // streaming query only
                                                     // Minimum number of offsets to be processed per trigger interval.
                // The specified total number of offsets will be proportionally split across topicPartitions of different volume.
                // Note, if the maxTriggerDelay is exceeded, a trigger will be fired even if the number of available offsets doesn't reach minOffsetsPerTrigger.

                .option("maxTriggerDelay", "15m")  // Maximum amount of time for which trigger can be delayed between two triggers
                // provided some data is available from the source. This option is only applicable if minOffsetsPerTrigger is set.




                .load();




    }
}
