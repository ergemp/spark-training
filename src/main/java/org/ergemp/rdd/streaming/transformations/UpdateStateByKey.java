package org.ergemp.rdd.streaming.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class UpdateStateByKey {
    public static void main(String[] args){
        Logger log = Logger.getRootLogger();
        log.setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("UpdateStateByKeyExample")
                .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));
        jssc.checkpoint("checkpoint/UpdateStateByKeyExample");

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 19999);

        JavaPairDStream<String, Long> wordCounts = lines
                .flatMap(line -> Arrays.asList(line.replaceAll("\\s+" , "\\s" ).split("\\s")).iterator())
                .mapToPair(line -> new Tuple2<>(line, 1L))
                .reduceByKey((a,b) -> a+b)
                ;

        JavaPairDStream<String, Long> updatedWordCounts = wordCounts.updateStateByKey(COMPUTE_RUNNING_SUM);

        updatedWordCounts.print();

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Function2<List<Long>, Optional<Long>, Optional<Long>>
        COMPUTE_RUNNING_SUM = (nums, current) -> {
        long sum = current.or(0L);
        for (long i : nums) {
            sum += i;
        }
        return Optional.of(sum);
    };
}

// ref:
// https://databricks.gitbooks.io/databricks-spark-reference-applications/content/logs_analyzer/chapter1/total.html

/*
* updateStateByKey(func)
*
* Return a new "state" DStream where the state for each key is updated by applying the given function
* on the previous state of the key and the new values for the key.
* This can be used to maintain arbitrary state data for each key.
* */