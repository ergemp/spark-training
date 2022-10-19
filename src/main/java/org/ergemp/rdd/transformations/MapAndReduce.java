package org.ergemp.rdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class MapAndReduce {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("MapAndReduce").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        //JavaRDD<Integer> distData = sc.parallelize(data);
        JavaRDD<Integer> distData = sc.parallelize(data,4);
        JavaRDD<String> distFile = sc.textFile("data/airlines/airports.dat",4);

        //add sizes of all lines
        distFile.map(s -> s.length()).reduce((a, b) -> a + b);
    }
}
