package org.ergemp.rdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class MapReduceWithCustomFunction {
    public static String COMMA_DELIMITER = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("MapReduceWithCustomFunction").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //passing functions instead of lambda expression
        JavaRDD<String> lines2 = sc.textFile("data/airlines/airports.dat");
        JavaRDD<Integer> lineLengths2 = lines2.map(new GetLength());
        int totalLength2 = lineLengths2.reduce(new Sum());

        System.out.println(totalLength2);
    }

    public static class GetLength implements Function<String, Integer> {
        public Integer call(String s) { return s.length(); }
    }
    public static class Sum implements Function2<Integer, Integer, Integer> {
        public Integer call(Integer a, Integer b) { return a + b; }
    }
}
