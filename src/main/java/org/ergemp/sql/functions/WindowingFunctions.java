package org.ergemp.sql.functions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class WindowingFunctions {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("JsonFunctions")
                .getOrCreate();

        //StructType schema = new StructType().add("customer_id","Integer");
        //Dataset<Row> df = spark.read().schema(schema).json("data/customerSales/spark_training_customers.json");

        Dataset<Row> df = spark.read().option("inferschema",true).json("data/customerSales/spark_training_sales.json");
        df.printSchema();


        // we can use any existing aggregate functions as a window function.

        // To perform an operation on a group first, we need to partition the data using Window.partitionBy(),
        // and for row number and rank function we need to additionally order by on partition data using orderBy clause.

        // row_number() window function is used to give the sequential row number
        // starting from 1 to the result of each window partition.


        WindowSpec windowSpec = Window.partitionBy("customer_id").orderBy("price_pp");

        df.withColumn("row_number", row_number().over(windowSpec)).show(100,false);

        // rank() window function is used to provide a rank to the result within a window partition.
        // This function leaves gaps in rank when there are ties.

        df.withColumn("rank", rank().over(windowSpec)).show();

        // dense_rank() window function is used to get the result with rank of rows within a window partition without any gaps.
        // This is similar to rank() function difference being rank function leaves gaps in rank when there are ties.

        df.withColumn("dense_rank", dense_rank().over(windowSpec)).show();

        // percent_rank() function

        df.withColumn("percent_rank", percent_rank().over(windowSpec)).show();

        // ntile() window function returns the relative rank of result rows within a window partition.
        // In below example we have used 2 as an argument to ntile hence it returns ranking between 2 values (1 and 2)

        df.withColumn("ntile", ntile(2).over(windowSpec)).show();

        // window analytic functions

        // cume_dist() window function is used to get the cumulative distribution of values within a window partition.
        // This is the same as the DENSE_RANK function in SQL.

        df.withColumn("cume_dist", cume_dist().over(windowSpec)).show();

        // lag() functions
        // This is the same as the LAG function in SQL.

        df.withColumn("lag", lag("price_pp", 2).over(windowSpec)).show();

        // lead() function
        // This is the same as the LEAD function in SQL.

        df.withColumn("lead", lead("price_pp", 2).over(windowSpec)).show();

        // window aggregate functions

        WindowSpec windowSpecAgg  = Window.partitionBy("customer_id");

        df.withColumn("row", row_number().over(windowSpec))
            .withColumn("avg", avg(col("price_pp")).over(windowSpecAgg))
            .withColumn("sum", sum(col("price_pp")).over(windowSpecAgg))
            .withColumn("min", min(col("price_pp")).over(windowSpecAgg))
            .withColumn("max", max(col("price_pp")).over(windowSpecAgg))
            .where(col("row").equalTo(1)).select("customer_id", "avg", "sum", "min", "max")
        .show();
        
    }
}
