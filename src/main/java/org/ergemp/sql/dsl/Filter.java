package org.ergemp.sql.dsl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class Filter {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Filter")
                .getOrCreate();

        //StructType schema = new StructType().add("customer_id","Integer");
        //Dataset<Row> df = spark.read().schema(schema).json("data/customerSales/spark_training_customers.json");

        Dataset<Row> df = spark.read().option("inferschema",true).json("data/customerSales/spark_training_customers.json");
        df.printSchema();

        // equalTo example
        df.filter(df.col("city").equalTo("Istanbul")).show();

        // multiple filter example
        // by default and
        df.filter(df.col("city").equalTo("Istanbul")).filter(df.col("country").equalTo("Netherlands")).show();

        // isInCollection example
        List<String> cities = new ArrayList<>();
        cities.add("Istanbul");
        cities.add("Amsterdam");

        df.filter(df.col("city").isInCollection(cities)).show();

        //startsWith example
        df.filter(df.col("city").startsWith("A")).show();

        //endsWith example
        df.filter(df.col("city").endsWith("bul")).show();

        //contains example
        df.filter(df.col("city").contains("bul")).show();

        //filter based on array columns
        //tbd

        // like example
        df.filter(df.col("email").like("%mtv%")).show();

        // rlike example
        df.filter(df.col("email").rlike("(?i)^*rose$")).show();


    }
}
