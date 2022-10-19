package org.ergemp.rdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Distinct {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("DistinctExample").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> data = Arrays.asList("the", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog", "jumped", "over", "the", "lazy", "dog");

        JavaRDD<String> distData = sc.parallelize(data);
        distData.distinct().foreach(line -> System.out.println(line));
    }
}
