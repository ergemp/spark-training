package org.ergemp.rdd.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

public class UsingSparkSQLwithForEachRDD {
    public static void main(String[] args) {

        Logger log = Logger.getRootLogger();
        log.setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("UsingSparkSQLwithForEachRDD")
                .setMaster("local[*]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));
        JavaDStream<String> lines = jssc.socketTextStream("localhost", 19999);

        JavaDStream<String> words = lines.flatMap(l -> Arrays.stream(l.split(" ")).iterator());


        words.foreachRDD((rdd, time) -> {
            // Get the singleton instance of SparkSession
            SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();

            // Convert RDD[String] to RDD[case class] to DataFrame
            JavaRDD<JavaRow> rowRDD = rdd.map(word -> {
                JavaRow record = new JavaRow();
                record.setWord(word);
                return record;
            });
            Dataset<Row> wordsDataFrame = spark.createDataFrame(rowRDD, JavaRow.class);

            // Creates a temporary view using the DataFrame
            wordsDataFrame.createOrReplaceTempView("words");

            // Do word count on table using SQL and print it
            Dataset<Row> wordCountsDataFrame =
                    spark.sql("select word, count(*) as total from words group by word");
            wordCountsDataFrame.show();
        });

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static class JavaRow implements java.io.Serializable {
        private String word;

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }
    }
}
