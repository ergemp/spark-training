package org.ergemp.sql.functions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.ergemp.rdd.sources.CreateRDDFromImmutableList;
import org.sparkproject.guava.collect.ImmutableList;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

public class ConvertStringToArrayColumn {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("ConvertStringToArrayColumn").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SparkSession spark = new SparkSession(jsc.sc());

        List<String> llist = ImmutableList.of(
                "ergem spark peker",
                "deniz su peker");

        JavaRDD<String> rdd = jsc.parallelize(llist);

        Dataset<Row> df =  spark.createDataset(llist, Encoders.STRING()).toDF();

        df.printSchema();
        df.show();

        Dataset<Row> df2 = df
                .select(split(col("value")," ").alias("value_array"))
                .drop("value");

        df2.printSchema();
        df2.show();
    }
}