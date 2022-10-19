package org.ergemp.rdd.transformations.pairRdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AggregateByKeyExample2 {
    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("AggregateByKeyExample2").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> data = Arrays.asList("quick-A","quick-B","quick-C","brown-A","brown-B","fox-A","fox-D");

        JavaRDD<String> distData = sc.parallelize(data);

        distData
                .mapToPair(line -> new Tuple2<String, String>(line.split("-")[0], line.split("-")[1]))
                .aggregateByKey
                        (
                        new ArrayList<String>(),
                            new Function2<List<String>, String, List<String>>() {
                                @Override
                                public List<String> call(List<String> v1, String v2) throws Exception {
                                    v1.add(v2);
                                    return v1;
                                }
                            },
                            new Function2<List<String>, List<String>, List<String>>() {
                                @Override
                                public List<String> call(List<String> v1, List<String> v2) throws Exception {
                                    return v1 ;
                                }
                            }
                        )
                .foreach(line -> System.out.println(line))
                ;
    }
}
