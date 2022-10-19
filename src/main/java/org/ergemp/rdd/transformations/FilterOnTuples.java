package org.ergemp.rdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class FilterOnTuples {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("FilterExampleOnTuple").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> data = Arrays.asList("the quick brown fox, jumped over the lazy dog");

        JavaRDD<String> distData = sc.parallelize(data);

        //define a custom filter function works on tuples
        Function<Tuple2<String, Integer>, Boolean> filterFunction = w -> (w._2 >= 5);

        //get rid of the comma
        //convert every word in the sentence to lines
        //map to pair the words as >word,character count>
        //filter on the char count
        //print out the contents
        distData
                .map(line -> line.replaceAll(",",""))
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(line -> new Tuple2<String, Integer>(line, line.length()))
                .filter(line -> line._2 >= 5)
                .foreach(line -> System.out.println(line._1 + " - " + line._2));

        //do the same via the custom filter function
        //print the tuple
        distData
                .map(line -> line.replaceAll(",",""))
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(line -> new Tuple2<String, Integer>(line, line.length()))
                .filter(filterFunction)
                .foreach(line -> System.out.println(line));
    }
}
