package org.ergemp.sql.dsl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UnionAndUnionAll {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("UnionAndUnionAll")
                .getOrCreate();

        //StructType schema = new StructType().add("customer_id","Integer");
        //Dataset<Row> df = spark.read().schema(schema).json("data/customerSales/spark_training_customers.json");

        Dataset<Row> df = spark.read().option("inferschema",true).json("data/customerSales/spark_training_customers.json");
        df.printSchema();

        System.out.println(df.count());
        System.out.println(df.union(df).count());

        // unionAll example
        // DataFrame unionAll() method is deprecated since PySpark “2.0.0” version and recommends using the union() method.
        System.out.println(df.unionAll(df).count());

        // merge without duplicates
        System.out.println(df.union(df).distinct().count());
    }
}
