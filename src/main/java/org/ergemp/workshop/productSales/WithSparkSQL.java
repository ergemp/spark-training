package org.ergemp.workshop.productSales;

import org.apache.spark.sql.SparkSession;

public class WithSparkSQL {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("WithSparkSQL")
                .getOrCreate();

        spark.read().option("header", "true").csv("resources/customerSales/spark_training_customers.csv").createOrReplaceTempView("customers");
        spark.read().option("header", "true").csv("resources/customerSales/spark_training_sales.csv").createOrReplaceTempView("sales");

        //customers.show(100,false);
        //sales.show(100,false);

        spark.sql("select city, count(*) from sales left join customers on (sales.customer_id = customers.customer_id) group by customers.city order by customers.city").show();

    }
}
