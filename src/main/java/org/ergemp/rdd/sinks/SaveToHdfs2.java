package org.ergemp.rdd.sinks;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SaveToHdfs2 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("SaveToHdfs2").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> distData = jsc.textFile("hdfs://<ip>:<port>/data/airlines/airports.dat");

        distData.saveAsTextFile("hdfs://<ip>:<port>/output/SaveToHdfs2");
    }
}
