package org.ergemp.rdd.transformations.pairRdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class AggregateByKeyExample {
    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("AggregateByKeyExample").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> data = Arrays.asList("the quick brown fox, jumped over the lazy dog");

        JavaRDD<String> distData = sc.parallelize(data);

        distData
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(line -> new Tuple2<String, Integer>(line, 1))
                .aggregateByKey
                        (
                        0,
                            new Function2<Integer, Integer, Integer>() {
                                @Override
                                public Integer call(Integer v1, Integer v2) throws Exception {
                                    return v1+v2  ;
                                }
                            },
                            new Function2<Integer, Integer, Integer>() {
                                @Override
                                public Integer call(Integer v1, Integer v2) throws Exception {
                                    return v1;
                                }
                            }
                        )
                .foreach(line -> System.out.println(line))
                ;
    }
}

/*

* aggregateByKey(zeroValue)(seqOp, combOp, [numPartitions])

* When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs
* where the values for each key are aggregated using the given combine functions
* and a neutral "zero" value.
*
* Allows an aggregated value type that is different than the input value type,
* while avoiding unnecessary allocations.
*
* Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
* */