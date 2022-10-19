package org.ergemp.rdd.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TakeAndForeach {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TakeAndForeachExample").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distFile = sc.textFile("hdfs://localhost:8020//mockdata/flumeData_181230_2000.1546189200983");
        distFile.take(100).forEach(data -> System.out.println(data));
    }
}
