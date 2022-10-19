package org.ergemp.sql.actions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Show {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Show")
                .getOrCreate();

        Dataset <Row> df =  spark.read().option("header", true).option("inferschema",true).csv("data/category.csv");

        df.show();
        // by default
        // df.show(20,true, false);
        df.show(100,false);

    }
}
