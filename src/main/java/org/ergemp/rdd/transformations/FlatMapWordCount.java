package org.ergemp.rdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FlatMapWordCount {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("FlatMapWordCountExample").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> data = Arrays.asList("hellori hazretleri bir gun yola giderken","hellori","hazretleri","bir gun","nedir");
        JavaRDD<String> distData = sc.parallelize(data);
        //JavaRDD<String> distData = sc.textFile("in/path.txt");

        JavaRDD<String> words = distData.flatMap(lines -> Arrays.asList(lines.split(" ")).iterator());
        Map<String, Long> wordCounts = words.countByValue();
        Long totalCounts = words.count();

        System.out.println("total lines: " + totalCounts + "\n ----------");

        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }
}
