package org.ergemp.rdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Map {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("MapExample").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> distData = jsc.textFile("data/airlines/airports.dat");

        distData.map(in -> in.split(",")[2])
                .filter(in -> in.contains("Ankara"))
                .foreach(in -> System.out.println(in));

        distData.filter(in -> in.contains("Ankara"))
                .map(in -> in.split(",")[2] + " - " + in.split(",")[3])
                .foreach(in -> System.out.println(in));

        //
        //
        //
        JavaRDD<String> distDataFiltered = distData.filter(in -> in.contains("Ankara"));

        JavaRDD<String> distDataFilteredMapped = distDataFiltered
                .map(in -> in.split(",")[2] + " - " + in.split(",")[3]);

        distDataFilteredMapped.foreach(in -> System.out.println(in));
    }
}
