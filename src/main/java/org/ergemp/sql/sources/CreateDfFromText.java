package org.ergemp.sql.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDfFromText {
    public static void main(String[] args) {
        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromText")
                .master("local")
                .getOrCreate();

        // read list to RDD
        String filePath = "data/test.csv";
        Dataset<Row> items = spark.read().text(filePath);

        items.printSchema();
        items.show();
    }
}
