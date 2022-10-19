package org.ergemp.rdd.actions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Map;

public class CountByKey {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("CountByKey").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = jsc.textFile("data/airlines/airports.dat");

        Map<String, Long> totalAirportsByCountry = rdd
                .map(line -> line.split(",")[3])
                .mapToPair(line -> {return new Tuple2<String, Integer>(line,1);})
                .countByKey()
                ;

        Iterator it = totalAirportsByCountry.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Long> entry = (Map.Entry<String, Long>)it.next();
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }
}
