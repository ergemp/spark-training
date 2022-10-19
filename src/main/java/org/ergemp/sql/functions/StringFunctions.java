package org.ergemp.sql.functions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;


public class StringFunctions {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("StringFunctions")
                .getOrCreate();

        //StructType schema = new StructType().add("customer_id","Integer");
        //Dataset<Row> df = spark.read().schema(schema).json("data/customerSales/spark_training_customers.json");

        Dataset<Row> df = spark.read().option("inferschema",true).json("data/customerSales/spark_training_sales.json");
        df.printSchema();

        // concat example
        df.select(concat(col("sales_id"), lit(" - "), col("product_id")).alias("concat_col")).show(1);

        // concat_ws example
        df
                .withColumn("concat_ws_col",
                        concat_ws(" - ",
                                col("sales_id"),
                                col("product_id")))
                .drop(col("sales_id"))
                .drop(col("product_id"))
                .show();

        df.createOrReplaceTempView("mytable");
        spark.sql("select concat(sales_id, ' - ', product_id) from mytable").show(1);
        spark.sql("select sales_id || ' - ' ||  product_id from mytable").show(1);

    }
}
