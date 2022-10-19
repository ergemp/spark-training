package org.ergemp.rdd.streaming.kafkaExamples;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KafkaProcessorWithManualOffsetsExample {
    public static void main(String[] args) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "ManualOffsets");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("ManualOffsets");

        SparkConf conf = new SparkConf().setAppName("KafkaProcessorWithManualOffsetsExample").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(1000));

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
        stream.foreachRDD(rdd -> printOffsets(rdd));

        try {
            streamingContext.start();
            streamingContext.awaitTermination();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void printOffsets(JavaRDD rdd){
        OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
        Arrays.stream(offsetRanges)
                .map(offsetRange -> offsetRange.partition() + " - " + offsetRange.fromOffset())
                .forEach(line -> System.out.println(line));

        String offsetStrings = Arrays.stream(offsetRanges)
                .map(offsetRange -> offsetRange.partition() + " - " + offsetRange.fromOffset()).toString();

        System.out.println(offsetStrings);
    }

    public static void saveOffsets(JavaRDD rdd){
        OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
        Arrays.stream(offsetRanges).map(offsetRange -> offsetRange.partition() + " " + offsetRange.fromOffset());
    }
}
