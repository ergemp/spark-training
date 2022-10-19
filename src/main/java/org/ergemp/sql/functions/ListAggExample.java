package org.ergemp.sql.functions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class ListAggExample {
    public static void main(String[] args) {

        /*
        select
             key,
             array_join( -- concat the array
              collect_list(code), -- aggregate that collects the array of [code]
              ' - ' -- delimiter
             )
        from demo_table
        group by KEY
        * */

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("ListAggExample")
                .getOrCreate();

        //StructType schema = new StructType().add("customer_id","Integer");
        //Dataset<Row> df = spark.read().schema(schema).json("data/customerSales/spark_training_customers.json");

        Dataset<Row> df = spark.read().option("inferschema",true).json("data/customerSales/spark_training_sales.json");
        df.createOrReplaceTempView("dftable");
        df.printSchema();

        //spark.sql("select customer_id, collect_list(product_id) from dftable group by customer_id").show(false);
        //spark.sql("select customer_id, array_join(collect_list(product_id),' - ') from dftable group by customer_id").show(false);

        df
            .groupBy(col("customer_id"))
            .agg(collect_list(col("product_id")))
            .show(false);
            ;

        df
                .groupBy(col("customer_id"))
                .agg(array_join(collect_list(col("product_id")), " - " ))
                .show(false);
        ;


    }
}
