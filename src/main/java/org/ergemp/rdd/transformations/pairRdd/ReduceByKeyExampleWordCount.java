package org.ergemp.rdd.transformations.pairRdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ReduceByKeyExampleWordCount {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("ReduceByKeyExampleWordCount").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> data = Arrays.asList("hellori hazretleri bir gun yola giderken","hellori","hazretleri","bir gun","nedir");
        JavaRDD<String> lines = sc.parallelize(data);
        JavaRDD<String> wordRdd = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        //JavaPairRDD<String, Integer> wordPairRdd = wordRdd.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordPairRdd = wordRdd.mapToPair(word -> new Tuple2<>(word, 1));

        //JavaPairRDD<String, Integer> wordCounts = wordPairRdd.reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> x + y);
        JavaPairRDD<String, Integer> wordCounts = wordPairRdd.reduceByKey((x, y) -> x + y);

        Map<String, Integer> worldCountsMap = wordCounts.collectAsMap();

        for (Map.Entry<String, Integer> wordCountPair : worldCountsMap.entrySet()) {
            System.out.println(wordCountPair.getKey() + " : " + wordCountPair.getValue());
        }
    }
}
