package org.ergemp.sql.dsl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Sample {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Sample")
                .getOrCreate();

        //StructType schema = new StructType().add("customer_id","Integer");
        //Dataset<Row> df = spark.read().schema(schema).json("data/customerSales/spark_training_customers.json");

        Dataset<Row> df = spark.read().option("inferschema",true).json("data/customerSales/spark_training_customers.json");
        df.printSchema();
        df.show();

        Dataset<Long> df2 = spark.range(100);
        df2.show();

        System.out.println(df.sample(0.6).count());
        df.sample(0.1).show();

        // Using seed to reproduce the same Samples
        df.sample(0.1,123L).show();
        df.sample(0.1,123L).show();
        df.sample(0.1,456L).show();
    }
}
