package org.ergemp.rdd.streaming.sources;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KafkaSource {
    public static void main(String[] args) {

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark_KafkaSource_example");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("mytopic");

        SparkConf conf = new SparkConf().setAppName("KafkaSource").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(10000));

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaPairDStream<String, String> pairStream = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        JavaDStream<String> lines = stream.map(ConsumerRecord::value);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.replaceAll("\\s*", " ").split(" ")).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();

        pairStream.print();

        pairStream.foreachRDD(rdd -> {
            //OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            //((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);

            rdd.collect().forEach(k -> { System.out.println("key: " + k._1 + " - value: " + k._2); });
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
          --class org.ergemp.training.spark.streaming.processors.KafkaProcessorExample \
          --master spark://localhost:7077 \
          --executor-memory 1G \
          --total-executor-cores 1 \
          spark_training-1.0-SNAPSHOT-jar-with-dependencies.jar \
          1000
        */
    }
}
