package org.ergemp.rdd.transformations.pairRdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class ReduceByKeyExample {
    public static void main(String[] args){

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("ReduceByKeyExample").setMaster("local[1]");
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
reduceByKey(func, [numPartitions])

When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs
where the values for each key are aggregated using the given reduce function func,
which must be of type (V,V) => V.

Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
*/