package org.ergemp.rdd.streaming.transformations;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

public class Reduce {
    public static void main(String[] args) {

        /* Return a new DStream of single-element RDDs by aggregating the elements in each RDD
         * of the source DStream using a function func (which takes two arguments and returns one).
         * The function should be associative and commutative so that it can be computed in parallel.
         * */

        Logger log = Logger.getRootLogger();
        log.setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        java.util.Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "SparkReduceExample");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("mytopic");

        SparkConf conf = new SparkConf()
                .setAppName("ReduceExample")
                .setMaster("local[*]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        stream.map(l -> l.value().length())
                        .reduce(new Function2<Integer, Integer, Integer>() {
                            @Override
                            public Integer call(Integer i1, Integer i2) throws Exception {
                                return i1+i2;
                            }
                        })
                .print();

        try {
            jssc.start();
            jssc.awaitTermination();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}
