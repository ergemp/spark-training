package org.ergemp.sql.structType;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class BasicStructType {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("BasicStructType")
                .getOrCreate();

        StructType schema = new StructType()
                .add("customer_id","Integer",true)
                .add("first_name","String")
                .add("last_name","String")
                .add("gender","String")
                .add("ip_address","String")
                .add("city","String")
                .add("country","String")
                ;

        Dataset<Row> df = spark.read().schema(schema).json("data/customerSales/spark_training_customers.json");
        df.show();
    }
}
