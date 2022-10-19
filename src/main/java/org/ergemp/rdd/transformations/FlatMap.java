package org.ergemp.rdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatMap {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("FlatMapExample").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distData = sc.parallelize(Arrays.asList("u1-p1,p2,p3","u2-p4,p5,p6","u3-p7,p8,p9"));

        distData
                .flatMap(line -> Arrays.asList(line.split("-")[1].split(",")).iterator())
                .foreach(line -> System.out.println(line))
        ;

        distData
                .flatMap(line -> {
                    List<String> retList = new ArrayList<>();

                    List<String> products = Arrays.asList(line.split("-")[1].split(",")) ;
                    for (String product:products) {
                        retList.add(line.split("-")[0] + " - " + product);
                    }

                    return retList.iterator();
                })
                .foreach(line -> System.out.println(line))
        ;
    }
}
