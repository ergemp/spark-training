package org.ergemp.rdd.transformations.pairRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class MapExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("MapExample").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String,String>> myTuple2 = Arrays.asList(
                new Tuple2<>("hellori","2"),
                new Tuple2<>("totos","4"),
                new Tuple2<>("topitop","13"));

        List<Tuple2<String,String>> myTuple3 = Arrays.asList(
                new Tuple2<>("hellori","mudur"),
                new Tuple2<>("totos","basgan"),
                new Tuple2<>("topitop","babus"));

        //mapValues maps the values part of the tuple and return pairRDD
        JavaPairRDD<String, String> myPairRDD3 = sc.parallelizePairs(myTuple3);
        JavaPairRDD<String, String> filtered3 = myPairRDD3.mapValues( f -> f.toUpperCase());
        filtered3.foreach(f -> System.out.println(f._2));

        //map function maps the keys ans returns an RDD
        JavaPairRDD<String, String> myPairRDD2 = sc.parallelizePairs(myTuple2);
        JavaRDD<String> filtered2 = myPairRDD2.map(f -> f._1.toUpperCase());
        filtered2.foreach(f -> System.out.println(f));
    }
}
