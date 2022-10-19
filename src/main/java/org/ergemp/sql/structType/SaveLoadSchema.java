package org.ergemp.sql.structType;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class SaveLoadSchema {

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
        // df.show();

        System.out.println(schema.json());
        System.out.println(schema.simpleString());

        /*
        {"type":"struct","fields":[{"name":"customer_id","type":"integer","nullable":true,"metadata":{}},{"name":"first_name","type":"string","nullable":true,"metadata":{}},{"name":"last_name","type":"string","nullable":true,"metadata":{}},{"name":"gender","type":"string","nullable":true,"metadata":{}},{"name":"ip_address","type":"string","nullable":true,"metadata":{}},{"name":"city","type":"string","nullable":true,"metadata":{}},{"name":"country","type":"string","nullable":true,"metadata":{}}]}
        struct<customer_id:int,first_name:string,last_name:string,gender:string,ip_address:string,city:string,country:string>
        */

        //StructType.fromDDL("");
        //StructType.fromJson("");
    }
}
