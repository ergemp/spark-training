package org.ergemp.sql.dsl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PivotUnPivotExample {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("PivotUnPivotExample")
                .getOrCreate();

        Dataset<Row> df = spark
                .read()
                .option("header",true)
                .csv("data/zipcodes/small_zipcodes.csv");

        df.printSchema();

        Dataset<Row> pivotDf = df.groupBy("state").pivot("city").count();
        pivotDf.show();

        /*
        Another approach is to do two-phase aggregation.
        PySpark 2.0 uses this implementation
        in order to improve the performance Spark-13749
        */
        df
            .groupBy("state", "city")
            .count()
                .groupBy("city")
                .pivot("state")
                .sum("count")
                .show();

    }
}
