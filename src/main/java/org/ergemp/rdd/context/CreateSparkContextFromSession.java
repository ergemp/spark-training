package org.ergemp.rdd.context;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class CreateSparkContextFromSession {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("CreateSparkContextFromSession")
                .setMaster("local[*]");

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);
    }
}
