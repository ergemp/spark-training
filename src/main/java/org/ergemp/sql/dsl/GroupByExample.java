package org.ergemp.sql.dsl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class GroupByExample {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("GroupByExample")
                .getOrCreate();

        //StructType schema = new StructType().add("customer_id","Integer");
        //Dataset<Row> df = spark.read().schema(schema).json("data/customerSales/spark_training_customers.json");

        Dataset<Row> df = spark.read().option("inferschema",true).json("data/customerSales/spark_training_customers.json");
        Dataset<Row> df2 = spark.read().option("inferschema",true).json("data/customerSales/spark_training_sales.json");

        df.printSchema();
        df2.printSchema();

        df.groupBy("country").count().show();
        df.groupBy("country").agg(sum(lit(1)).alias("total")).where(col("total").gt(10)).show();

        df2.groupBy("customer_id").max("price_pp").show();
        df2.groupBy("customer_id").min("price_pp").show();
        df2.groupBy("customer_id").avg("price_pp").show();
        df2.groupBy("customer_id").mean("price_pp").show();

        df2.groupBy("customer_id").agg(
            max("price_pp").alias("max_price"),
            min("price_pp").alias("min_price"),
            avg("price_pp").alias("avg_price"),
            mean("price_pp").alias("mean_price"),
            sum("price_pp").alias("sum_price"),
            count("price_pp").alias("count_price")
        ).show();

    }
}
