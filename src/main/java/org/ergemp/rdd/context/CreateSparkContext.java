package org.ergemp.rdd.context;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class CreateSparkContext {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CreateSparkContext").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
    }
}
