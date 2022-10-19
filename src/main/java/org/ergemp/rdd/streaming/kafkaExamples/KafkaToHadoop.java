package org.ergemp.rdd.streaming.kafkaExamples;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KafkaToHadoop {
    public static void main(String[] args) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark_KafkaToHadoop_example");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("mytopic");

        SparkConf conf = new SparkConf().setAppName("KafkaToHadoop").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(10000));

        JavaInputDStream<ConsumerRecord<String, String>> kafkaMsg =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        //kafkaMsg.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        JavaDStream<String> values = kafkaMsg.map(ConsumerRecord::value);
        JavaDStream<String> keys = kafkaMsg.map(ConsumerRecord::key);

        values.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            ((CanCommitOffsets) kafkaMsg.inputDStream()).commitAsync(offsetRanges);

            //rdd.collect().forEach(a -> { System.out.println(a); });
            if (rdd.count() > 0)
            {
                rdd.saveAsTextFile("hdfs://localhost:8020/kafka/mytopic");
            }
            else
            {
                System.out.println("no new data in topic");
            }

        });

        try {
            streamingContext.start();
            streamingContext.awaitTermination();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }

        /*
        /usr/local/spark/bin/spark-submit \
          --class org.ergemp.training.spark.streaming.processors.KafkaToHadoop \
          --master spark://localhost:7077 \
          --executor-memory 1G \
          --total-executor-cores 1 \
          spark_training-1.0-SNAPSHOT-jar-with-dependencies.jar \
          1000
        */
    }
}
