package org.ergemp.rdd.actions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class ForeachExampleWithCustomFunction {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("ForeachExampleWithCustomFunction").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> distData = sc.parallelize(data);

        //rather than transformations, actions doesnt return an rdd anymore
        distData.foreach(line -> System.out.println(line));

        distData.foreach(CustomForEach.customForEachFunction);
    }
    static class CustomForEach {
        public static VoidFunction<Integer> customForEachFunction = new VoidFunction<Integer>() {
            @Override
            public void call(Integer s) {
                System.out.println(" * "+ s);
            }
        };
    }
}
