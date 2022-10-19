package org.ergemp.sql.actions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class Collect {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Collect")
                .getOrCreate();

        StructType schema = new StructType().add("customer_id","Integer");

        Dataset<Row> df = spark.read().schema(schema).json("data/customerSales/spark_training_customers.json");
        df.show();

        Row[] resultsArray =  (Row[]) df.collect();
        List<Row> resultsList =  df.collectAsList();

        System.out.println(resultsArray.length);
        System.out.println(resultsList.size());

        for (Integer i=0; i<resultsArray.length; i++) {
            System.out.println(resultsArray[i]);
        }

        for (Row row :resultsList) {
            System.out.println(row.get(row.fieldIndex("customer_id")));
        }
    }
}
