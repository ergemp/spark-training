package org.ergemp.sql.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDfFromCsvHadoop {
    public static void main(String[] args) {
        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromCsvHadoop")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true") //first line in file has headers
                .option("mode", "DROPMALFORMED")
                .load("hdfs:///csv/file/dir/file.csv");

        Dataset<Row> df2 = spark.sql("SELECT * FROM csv.'hdfs:///csv/file/dir/file.csv'");
    }
}
