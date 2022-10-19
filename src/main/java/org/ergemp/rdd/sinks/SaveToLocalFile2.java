package org.ergemp.rdd.sinks;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SaveToLocalFile2 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("SaveToLocalFile2").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = jsc.textFile("data/airlines/airports.dat");

        rdd
                .map(line -> line.split(",")[3])
                .saveAsTextFile("output/SaveAsTextFileExample2-1.out");

        rdd
                .map(line -> line.split(",")[3])
                .mapToPair(line -> {return new Tuple2<String, Long>(line,1L);})
                .reduceByKey((a,b) -> a+b)
                .mapToPair(line -> {return new Tuple2<Long,String>(line._2, line._1);})
                .sortByKey()
                .mapToPair(line -> {return new Tuple2<String,Long>(line._2, line._1);})
                .saveAsTextFile("output/SaveAsTextFileExample2-2.out");
    }
}
