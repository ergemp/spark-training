package org.ergemp.sql.dsl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class JoinExample {
    public static void main(String[] args) {
        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .appName("JoinExample")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> airlinesDF = spark.read().csv("data/airlines/airlines.dat")
                .withColumnRenamed("_c0","id")
                .withColumnRenamed("_c1","name")
                .withColumnRenamed("_c2","alias")
                .withColumnRenamed("_c3","iata")
                .withColumnRenamed("_c4","icao")
                .withColumnRenamed("_c5","callsign")
                .withColumnRenamed("_c6","country")
                .withColumnRenamed("_c7","active")
                ;

        Dataset<Row> countriesDF = spark.read().csv("data/airlines/countries.dat")
                .withColumnRenamed("_c0","name")
                .withColumnRenamed("_c1","isoCode")
                .withColumnRenamed("_c2","dafifCode")
                ;

        // inner join
        Dataset<Row> innerJoin = airlinesDF.join(countriesDF,airlinesDF.col("country").equalTo(countriesDF.col("name")),"inner");

        //drop column after join
        airlinesDF
                .join(countriesDF,
                        airlinesDF.col("country").equalTo(countriesDF.col("name")),
                "inner")
                .drop(countriesDF.col("name"))
                .show();

        airlinesDF.explain();
        airlinesDF.show(100, false);

        //full outer
        //outer a.k.a full, fullouter
        Dataset<Row> outerJoin = airlinesDF.join(countriesDF,airlinesDF.col("country").equalTo(countriesDF.col("name")),"outer");
        Dataset<Row> fullJoin = airlinesDF.join(countriesDF,airlinesDF.col("country").equalTo(countriesDF.col("name")),"full");
        Dataset<Row> fullOuterJoin = airlinesDF.join(countriesDF,airlinesDF.col("country").equalTo(countriesDF.col("name")),"fullouter");

        //left outer
        Dataset<Row> leftJoin = airlinesDF.join(countriesDF,airlinesDF.col("country").equalTo(countriesDF.col("name")),"left");
        Dataset<Row> leftOuterJoin = airlinesDF.join(countriesDF,airlinesDF.col("country").equalTo(countriesDF.col("name")),"leftouter");

        //right outer
        Dataset<Row> rightJoin = airlinesDF.join(countriesDF,airlinesDF.col("country").equalTo(countriesDF.col("name")),"right");
        Dataset<Row> rightOuterJoin = airlinesDF.join(countriesDF,airlinesDF.col("country").equalTo(countriesDF.col("name")),"rightouter");

        //self join example
        Dataset<Row> selfJoin = airlinesDF.alias("airlines1")
                .join(airlinesDF.alias("airlines2"), col("airlines1.country").equalTo(col("airlines2.country")),"inner")
                .select("airlines1.country");
        selfJoin.show();

        // join with raw sql
        airlinesDF.createOrReplaceTempView("airlines");
        countriesDF.createOrReplaceTempView("countries");

        spark.sql("select * from airlines join countries on (airlines.country=countries.name)").show();

    }
}
