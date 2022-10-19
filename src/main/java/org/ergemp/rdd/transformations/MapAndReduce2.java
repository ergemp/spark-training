package org.ergemp.rdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MapAndReduce2 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("MapAndReduce2").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //defining transformation(map) and actions(reduce)
        JavaRDD<String> lines = sc.textFile("data/airlines//airports.dat");
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);

        System.out.println(totalLength);
    }
}
