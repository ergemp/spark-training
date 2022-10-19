package org.ergemp.rdd.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class FilterWithInlineFunction {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("FilterWithInlineFunction").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("data/airlines/airports.dat");
        JavaRDD<String> airportsUS = airports.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                return line.matches(".*United States.*");
            }
        });

        System.out.println(airportsUS.count());

        //OR
        //JavaRDD<String> airportsUS2 = airports.filter(new MatchUS());
    }
}
