package org.ergemp.rdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Union {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("UnionExample").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> airportsData = jsc.textFile("data/airlines/airports.dat");
        JavaRDD<String> deneme100 = jsc.textFile("data/deneme100.csv");

        airportsData.union(deneme100).sample(false,0.1,1).foreach(line -> System.out.println(line));

    }
}
