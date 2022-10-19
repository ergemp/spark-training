package org.ergemp.rdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class FilterOnNumericTypes {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("FilterOnNumericTypes").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //create a List of Integers for sample data
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);

        //parallelize the List and generate an RDD
        JavaRDD<Integer> distData = sc.parallelize(data);

        //create a custom filter function
        Function<Integer, Boolean> filter = k -> ( k % 3 == 0);

        //transform(filter) and act(foreach) on the RDD
        distData.filter(filter).foreach(line -> System.out.println(line));
    }
}
