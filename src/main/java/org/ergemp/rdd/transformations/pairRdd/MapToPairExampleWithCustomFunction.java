package org.ergemp.rdd.transformations.pairRdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class MapToPairExampleWithCustomFunction {
    public static void main(String[] args){

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("MapToPairExampleWithCustomFunction").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> data = Arrays.asList("the quick brown fox, jumped over the lazy dog");

        JavaRDD<String> distData = sc.parallelize(data);
        distData
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(CustomKeyValuePair.customKeyValuePairFunction)
                .foreach(line -> System.out.println("key: " + line._1 + " - " + "value: " + line._2 ) )
                ;

        distData
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(new CustomKeyValuePair2())
                .foreach(line -> System.out.println("key: " + line._1 + " - " + "value: " + line._2 ) )
                ;
    }

    //two different implementation for overriding the PairFunction
    //
    //create a new function instance from PairFunction
    //to call the function CustomKeyValuePair.customKeyValuePairFunction
    static class CustomKeyValuePair {
        public static PairFunction<String, String, Integer> customKeyValuePairFunction = new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        };
    }

    //or, Implement PairFunction
    //to call the class create an instance new CustomKeyValuePair2()
    public static class CustomKeyValuePair2 implements PairFunction<String, String, Integer>
    {
        @Override
        public Tuple2<String, Integer> call(String in) {
            return new Tuple2<String, Integer>(in, 1);
        }
    }

}




