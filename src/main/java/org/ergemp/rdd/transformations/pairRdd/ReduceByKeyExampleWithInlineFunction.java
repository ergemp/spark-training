package org.ergemp.rdd.transformations.pairRdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class ReduceByKeyExampleWithInlineFunction {
    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("ReduceByKeyExampleWithInlineFunction").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> data = Arrays.asList("the quick brown fox, jumped over the lazy dog");

        JavaRDD<String> distData = sc.parallelize(data);
        distData
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(line -> new Tuple2<String, Integer>(line, 1))
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer+integer2;
                    }
                })
                .foreach(line -> System.out.println(line))
        ;
    }
}
