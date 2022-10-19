package org.ergemp.rdd.streaming.kafkaExamples;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KafkaProcessorWithCheckpointDirExample {
    public String bootstrap_servers = "localhost:9092";
    public String group_id = "KafkaProcessorWithCheckpointDirExample-01";
    public String auto_offset_reset = "latest";
    public Boolean enable_auto_commit = false;

    public String topics = "hello-kafka-topic";
    public String app_name = "KafkaProcessorWithCheckpointDirExample-01";

    public JavaStreamingContext streamingContext;
    //public JavaSparkContext sc ;

    public JavaInputDStream<ConsumerRecord<String, String>> GetStream() {
        Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.ERROR);

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootstrap_servers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", group_id);
        kafkaParams.put("auto.offset.reset", auto_offset_reset);
        kafkaParams.put("enable.auto.commit", enable_auto_commit);

        Collection<String> topics = Arrays.asList(this.topics);

        //SparkConf conf = new SparkConf().setAppName(appName);
        SparkConf conf = new SparkConf()
                .setAppName(app_name)
                //It is important to ensure that the batch processing time is shorter than the batch interval
                .set("spark.streaming.backpressure.enabled", "true")
                .set("spark.streaming.backpressure.initialRate", "10000")
                .setMaster("local[4]");

        //this.sc = new JavaSparkContext(conf);
        this.streamingContext = new JavaStreamingContext(conf, new Duration(60000)); //Durations.seconds(60)
        this.streamingContext.checkpoint("hdfs://12.0.7.71:8020/user/spark/checkpoint/" + app_name); //this.streamingContext.sparkContext().appName()

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        this.streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
        return stream;
    }
}
