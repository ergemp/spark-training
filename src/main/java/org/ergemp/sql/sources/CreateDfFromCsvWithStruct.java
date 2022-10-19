package org.ergemp.sql.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class CreateDfFromCsvWithStruct {
    public static void main(String[] args) {
        Logger myLogger = Logger.getRootLogger();
        myLogger.setLevel(Level.ERROR);

        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromCsvWithStruct")
                .master("local")
                .getOrCreate();

        StructType AirportSchema = new StructType()
                .add("AirportID", "string")
                .add("Name", "string")
                .add("City", "string")
                .add("Country", "string")
                .add("IATA", "string")
                .add("ICAO", "string")
                .add("Latitude", "string")
                .add("Longitude", "string")
                .add("Altitude", "string")
                .add("Timezone", "string")
                .add("DST", "string")
                .add("Type", "string")
                ;

        StructType AirlineSchema = new StructType()
                .add("AirlineID", "string")
                .add("Name", "string")
                .add("Alias", "string")
                .add("IATA", "string")
                .add("ICAO", "string")
                .add("CallSign", "string")
                .add("Country", "string")
                .add("Active", "string")
                ;

        Dataset<Row> airports = spark.read().option("header", "false").schema(AirportSchema).csv("data/airlines/airports.dat");
        airports.createOrReplaceTempView("airports");


        Dataset<Row> airportsUSA = spark.sql("SELECT * FROM airports where lower(country)='iceland' limit 100");

        airportsUSA.printSchema();
        airportsUSA.show();
    }
}
