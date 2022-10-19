package org.ergemp.rdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class CoalesceAndRepartition {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("CoalesceAndRepartition").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> data = Arrays.asList("the", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog", "jumped", "over", "the", "lazy", "dog");

        JavaRDD<String> distData = sc.parallelize(data);

        /* Decrease the number of partitions in the RDD to numPartitions.
        Useful for running operations more efficiently after filtering down a large dataset. */
        distData.coalesce(1);

        /* Reshuffle the data in the RDD randomly to create either more or fewer partitions
        and balance it across them. This always shuffles all data over the network. */
        distData.repartition(6);
    }
}
