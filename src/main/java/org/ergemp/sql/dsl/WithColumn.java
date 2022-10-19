package org.ergemp.sql.dsl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class WithColumn {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Select")
                .getOrCreate();

        //StructType schema = new StructType().add("customer_id","Integer");
        //Dataset<Row> df = spark.read().schema(schema).json("data/customerSales/spark_training_customers.json");

        Dataset<Row> df = spark.read().option("inferschema",true).json("data/customerSales/spark_training_sales.json");
        df.printSchema();

        //create new column with cast data type
        df.withColumn("customer_id2",col("customer_id").cast("Integer")).printSchema();

        // Update The Value of an Existing Column
        df.withColumn("price_pp",col("price_pp").$times(-1)).show();

        // Create a Column from an Existing
        df.withColumn("CopiedColumn", col("price_pp").$times(-1)).show();

        //Add a New Column using withColumn()
        df.withColumn("Country", lit("USA")).show();
        df.withColumn("Country", lit("USA"))
            .withColumn("anotherColumn", lit("anotherValue")).show();

        //Rename Column Name
        df.withColumnRenamed("sales_id", "salesId").show();

        //Drop a Column
        df.drop("ts").show();
    }
}
