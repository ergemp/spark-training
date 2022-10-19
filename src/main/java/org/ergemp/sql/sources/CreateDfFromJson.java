package org.ergemp.sql.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDfFromJson {
    public static void main(String[] args) {
        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromJson")
                .master("local")
                .getOrCreate();

        // read list to RDD
        String jsonPath = "data/sample.json";
        Dataset<Row> df = spark.read().json(jsonPath);

        df.printSchema();
    }
}
