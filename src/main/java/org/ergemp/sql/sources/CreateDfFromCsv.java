package org.ergemp.sql.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDfFromCsv {
    public static void main(String[] args) {

        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromCsv")
                .master("local")
                .getOrCreate();

        String csvPath = "data/nasa-weblogs.txt";

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("delimiter","\t")
                .option("header", "true")
                .option("mode", "DROPMALFORMED")
                .load(csvPath);

        df.printSchema();
        df.show(100,false);
    }
}
