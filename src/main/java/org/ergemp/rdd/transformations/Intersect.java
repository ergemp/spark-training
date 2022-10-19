package org.ergemp.rdd.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Intersect {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("IntersectExample").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> file1 = sc.textFile("resources/nasa_19950630.22-19950728.12.tsv").map(line -> line.split("\\t")[0]);
        JavaRDD<String> file2 = sc.textFile("resources/nasa_19950731.22-19950831.22.tsv").map(line -> line.split("\\t")[0]);

        file1.intersection(file2)
                .filter(line -> isNotHeader(line))
                .sample(true, 0.1)
                .saveAsTextFile("output/intersectExample.out");
    }

    private static Boolean isNotHeader(String line) {
        return (!line.startsWith("host"));
    }
}
