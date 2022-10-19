package org.ergemp.rdd.transformations.pairRdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SortByKeyExample {
    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("SortByKeyExample").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> distData = jsc.textFile("data/airlines/airports.dat");

        distData.mapToPair(in -> new Tuple2<>(in.split(",")[2], 1))
                .reduceByKey((x,y) -> x+y)
                .mapToPair(in -> new Tuple2<>(in._2, in._1))
                .sortByKey(false)
                .take(10)
                .forEach(in -> System.out.println(in));

        //.foreach(in -> System.out.println(in))
        //;
    }
}

/*
sortByKey([ascending], [numPartitions])

When called on a dataset of (K, V) pairs where K implements Ordered,
returns a dataset of (K, V) pairs sorted by keys in ascending or descending order,
as specified in the boolean ascending argument.
*/
