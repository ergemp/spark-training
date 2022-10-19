package org.ergemp.sql.dsl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.broadcast;

public class JoinBroadcast {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JoinBroadcast")
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

        airlinesDF.show(100,false);
        countriesDF.show(100,false);

        Dataset<Row> joined = airlinesDF.join(broadcast(countriesDF),airlinesDF.col("country").equalTo(countriesDF.col("name")));
        joined.explain();
        joined.show(100,false);
    }
}
