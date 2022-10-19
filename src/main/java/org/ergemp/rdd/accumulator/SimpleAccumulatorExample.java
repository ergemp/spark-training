package org.ergemp.rdd.accumulator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Option;

public class SimpleAccumulatorExample {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("SimpleAccumulatorExample").setMaster("local[*]");
        SparkContext sparkContext = new SparkContext(conf);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

        final LongAccumulator totalLines = new LongAccumulator();
        final LongAccumulator totalBytes = new LongAccumulator();
        totalLines.register(sparkContext, Option.apply("totalLines"), false);
        totalBytes.register(sparkContext, Option.apply("totalBytes"), false);

        JavaRDD<String> file1 = javaSparkContext.textFile("data/nasa-weblogs.txt");

        file1.foreach(line -> {
            totalLines.add(1);
            totalBytes.add(line.getBytes().length);
        });

        System.out.println(totalLines);
        System.out.println(totalBytes);

        System.out.println("Avg Bytes per line: " + totalBytes.value()/totalLines.value());
    }
}
