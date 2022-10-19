package org.ergemp.rdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class JoinLeft {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("JoinExample").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> people = Arrays.asList("1,Shyama,Patni", "2,Paul,Kam", "3,Jayesh,Patel", "4,Sid,Dave", "5,Prakash,Aarya", "6,Jastin,Cohelo", "6,Cahit,Arf", "7,Muhittin,Gokmen");
        List<String> location = Arrays.asList("1,Kolkata,W.Bengal,India", "2,Atlanta,Georgia,USA", "3,Rajkot,Gujarat,India", "4,Mumbai,Maharashtra,India", "6,San Francisco,California,USA");

        JavaRDD<String> dPeople = sc.parallelize(people);
        JavaRDD<String> dLocation = sc.parallelize(location);

        //JavaPairRDD<String, Tuple2<String, Optional<String>>> joinOutput =
        JavaPairRDD<String, Iterable<Tuple2<String, Optional<String>>>> joinOutput =

                dPeople.mapToPair(line -> new Tuple2<String,String>(line.split(",")[0], line))
                        .leftOuterJoin(dLocation.mapToPair(line -> new Tuple2<String, String>(line.split(",")[0], line)))
                        .groupByKey()
                ;
        joinOutput.sortByKey().foreach(line -> System.out.println(line));

        /*handling null values*/
        JavaPairRDD<String, Iterable<Tuple2<String, Optional<String>>>> joinOutput2 =

                dPeople.mapToPair(line -> new Tuple2<String,String>(line.split(",")[0], line))
                        .leftOuterJoin(dLocation.mapToPair(line -> new Tuple2<String, String>(line.split(",")[0], line)))

                        //.mapToPair(line -> new Tuple2<String, String>(line._1 ,line._2._1.toString() + "," + line._2._2.orElse("null").toString()))

                        .groupByKey()
                ;
        joinOutput2.sortByKey().foreach(line -> System.out.println(line));
    }
}
