package org.ergemp.rdd.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Union2 {
    public static void main(String[] args) {
        //examples:
        //
        //sample
        //distinct
        //
        //union
        //intersection
        //substract
        //cartesian product

        SparkConf conf = new SparkConf().setAppName("rdd-setOperationsTest").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> file1 = sc.textFile("resources/nasa_19950630.22-19950728.12.tsv");
        JavaRDD<String> file2 = sc.textFile("resources/nasa_19950731.22-19950831.22.tsv");

        JavaRDD<String> unionedFile = file1.union(file2);
        JavaRDD<String> cleanedUnionedFile = unionedFile.filter(line -> isNotHeader(line));
        JavaRDD<String> sample = cleanedUnionedFile.sample(true, 0.1);
        sample.saveAsTextFile("out/setOperation.out");

        /*
        file1.union(file2)
             .filter(line -> isNotHeader(line))
             .sample(true, 0.1)
             .saveAsTextFile("sampleout/setOperation.out");
        */
    }

    private static Boolean isNotHeader(String line) {
        return (!line.startsWith("host"));
    }
}
