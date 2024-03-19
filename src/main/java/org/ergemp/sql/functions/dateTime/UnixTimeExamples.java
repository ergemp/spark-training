package org.ergemp.sql.functions.dateTime;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class UnixTimeExamples {
    public static void main(String[] args) {

        // https://sparkbyexamples.com/pyspark/pyspark-sql-working-with-unix-time-timestamp/

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("AddIntervalToDate")
                .getOrCreate();


    }
}
