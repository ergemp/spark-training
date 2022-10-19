package org.ergemp.sql.sinks;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SaveToTextFile {
    public static void main(String[] args) {
        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("SaveToTextFile")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true") //first line in file has headers
                .option("mode", "DROPMALFORMED")
                .load("data/test.csv");

        df.write().mode(SaveMode.Overwrite).csv("output/SaveToTextFile.csv");
        df.write().mode(SaveMode.Overwrite).json("output/SaveToTextFile.json");
    }
}
