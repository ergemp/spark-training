package org.ergemp.sql.colAndRowExamples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.lit;

public class CreateSparkCol {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("CreateSparkCol")
                .getOrCreate();

        //StructType schema = new StructType().add("customer_id","Integer");
        //Dataset<Row> df = spark.read().schema(schema).json("data/customerSales/spark_training_customers.json");

        Dataset<Row> df = spark.read().option("inferschema",true).json("data/customerSales/spark_training_customers.json");

        // with lit function
        df.select(df.col("*"), lit("tt").as("newColumn")).show(100,false);
        df.select(df.col("*"), lit("tt").as("newColumn")).printSchema();

        // with withColumn method
        df.select(df.col("*")).withColumn("newColName", lit(1)).show();
        df.printSchema();
    }
}
