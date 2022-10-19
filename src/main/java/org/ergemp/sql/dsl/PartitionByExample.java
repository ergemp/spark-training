package org.ergemp.sql.dsl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class PartitionByExample {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("PartitionByExample")
                .getOrCreate();

        Dataset<Row> df = spark
                .read()
                .option("header",true)
                .csv("data/zipcodes/small_zipcodes.csv");

        df.printSchema();


        df
            .write()
            .option("header", true)
            .partitionBy("state")
            .mode(SaveMode.Overwrite)
            .csv("output/PartitionByExample.csv");

        df
                .write()
                .option("header", true)
                .partitionBy("state","city")
                .mode(SaveMode.Overwrite)
                .csv("output/PartitionByExample.csv");

        // using rePartition with partitionBy
        df
                .repartition(2)
                .write()
                .option("header", true)
                .partitionBy("state","city")
                .mode(SaveMode.Overwrite)
                .csv("output/PartitionByExample.csv");

        // control number of records for each partition
        df
                .write()
                .option("header", true)
                .option("maxRecordsPerFile",2)
                .partitionBy("state")
                .mode(SaveMode.Overwrite)
                .csv("output/PartitionByExample.csv");

        //read a specific partition
        Dataset<Row> dfPart = spark.read().option("header",true).csv("output/PartitionByExample.csv/state=PR");
        dfPart.printSchema();

        Dataset<Row> dfPart2 = spark.read().option("header",true).csv("output/PartitionByExample.csv");
        dfPart2.createOrReplaceTempView("dfPart2");

        spark.sql("select * from dfPart2 where state='PR'").show();
    }
}
