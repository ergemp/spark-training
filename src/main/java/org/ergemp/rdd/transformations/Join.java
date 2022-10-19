package org.ergemp.rdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Join {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("JoinExample").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> people = Arrays.asList("1,Shyama,Patni", "2,Paul,Kam", "3,Jayesh,Patel", "4,Sid,Dave", "5,Prakash,Aarya", "6,Jastin,Cohelo", "6,Doe,John");
        List<String> location = Arrays.asList("1,Kolkata,W.Bengal,India", "2,Atlanta,Georgia,USA", "3,Rajkot,Gujarat,India", "4,Mumbai,Maharashtra,India", "6,San Francisco,California,USA");

        JavaRDD<String> dPeople = sc.parallelize(people);
        JavaRDD<String> dLocation = sc.parallelize(location);


        JavaPairRDD<String, Tuple2<String, String>> joinOutput = dPeople
                .mapToPair(line -> new Tuple2<String,String>(line.split(",")[0] ,line))
                .join(dLocation.mapToPair(line -> new Tuple2<String, String>(line.split(",")[0],line)))
                ;
        joinOutput.sortByKey().foreach(line -> System.out.println(line));
    }
}
