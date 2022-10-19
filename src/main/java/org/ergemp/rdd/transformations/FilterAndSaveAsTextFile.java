package org.ergemp.rdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FilterAndSaveAsTextFile {
    public static String COMMA_DELIMITER = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("FilterAndSaveAsTextFile").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("data/airlines/airports.dat");
        JavaRDD<String> airportsHigher40Lat = airports.filter(line -> Double.parseDouble(line.split(COMMA_DELIMITER)[6]) > 40)
                .map(line -> line.split(",")[1] + "," + line.split(",")[2] + "," + line.split(",")[3]);

        airportsHigher40Lat.saveAsTextFile("output/FilterAndSaveAsTextFile.out");
    }
}
