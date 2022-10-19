package org.ergemp.rdd.actions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class ForeachPartitionExampleWithLambda {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("ForeachPartitionExampleWithLambda").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1,2,3,4,5,6,7,8,9);

        JavaRDD<Integer> distData = sc.parallelize(data).repartition(4);

        //rather than transformations, actions does not return an rdd anymore
        distData.foreachPartition(part -> {
            while (part.hasNext()){
                Integer next = (Integer)part.next();
                System.out.println(part.hashCode() + " - " + next);
            }
        });
    }
}
