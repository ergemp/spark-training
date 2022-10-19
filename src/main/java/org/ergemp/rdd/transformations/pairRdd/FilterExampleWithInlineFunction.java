package org.ergemp.rdd.transformations.pairRdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class FilterExampleWithInlineFunction {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("FilterExampleWithInlineFunction").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String,String>> myTuple2 = Arrays.asList(
                new Tuple2<>("hellori","2"),
                new Tuple2<>("totos","4"),
                new Tuple2<>("topitop","13"));

        JavaPairRDD<String, String> myPairRDD = sc.parallelizePairs(myTuple2);
        JavaPairRDD<String, String> filtered = myPairRDD.filter(f -> !f._1.contains("hellori"));

        JavaPairRDD<String, String> filtered2 = myPairRDD.filter(new Function<Tuple2<String,String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String,String> line) throws Exception {
                return line._1.matches(".*totos.*");
            }
        });

        filtered2.foreach(f -> System.out.println(f._1));
        filtered.foreach(f -> System.out.println(f._1));
    }
}
