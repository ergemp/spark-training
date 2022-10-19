package org.ergemp.rdd.transformations.pairRdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class ConvertToPairRDDWithCustomFunction {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        Tuple2<String, String> myTuple = new Tuple2<>("this is key","this is value");
        String key = myTuple._1;
        String value = myTuple._2;

        SparkConf conf = new SparkConf().setAppName("ConvertToPairRDDWithCustomFunction").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String,String>> myTuple2 = Arrays.asList(new Tuple2<>("",""),
                new Tuple2<>("",""),
                new Tuple2<>("",""));

        List<String> myStringList = Arrays.asList("hellori 2", "totos 4", "topitop 13");

        JavaPairRDD<String, String> myPairRDD = sc.parallelizePairs(myTuple2);
        myPairRDD.coalesce(1).saveAsTextFile("output/pairRDDs.txt");

        JavaRDD<String> stringRDD = sc.parallelize(myStringList);
        JavaPairRDD<String, Integer> stringPairRDD = stringRDD.mapToPair(convertToPair());
        stringPairRDD.coalesce(1).saveAsTextFile("output/convertPairRDDs.txt");
    }

    public static PairFunction<String, String, Integer> convertToPair() {
        return (PairFunction<String, String, Integer>) s -> new Tuple2<>(s.split(" ")[0] , Integer.parseInt(s.split(" ")[1]));
    }
}
