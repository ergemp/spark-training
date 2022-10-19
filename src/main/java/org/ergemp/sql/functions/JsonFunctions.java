package org.ergemp.sql.functions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.sparkproject.guava.collect.ImmutableList;

import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

public class JsonFunctions {
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

        String jsonString = "{'Zipcode':704,'ZipCodeType':'STANDARD','City':'PARC PARQUE','State':'PR'}";

        Row r1 = RowFactory.create(jsonString);
        List<Row> rowList = ImmutableList.of(r1);

        StructType schema = DataTypes.createStructType(
                new StructField[]{
                        createStructField("value", StringType, false)
                });

        StructType schema2 = DataTypes.createStructType(
                new StructField[]{
                        createStructField("ZipCode", IntegerType, false),
                        createStructField("ZipCodeType", StringType, false),
                        createStructField("City", StringType, false),
                        createStructField("State", StringType, false)
                });

        Dataset<Row> dft = spark.createDataFrame(rowList,schema);
        dft.printSchema();
        dft.show(false);

        // to_json() example
        // convert DataFrame columns MapType or Struct type to JSON string

        schema2.printTreeString();
        String schema2JsonString = schema2.json();
        String schema2PrettyJsonString = schema2.prettyJson();

        System.out.println(schema2PrettyJsonString);
        System.out.println(DataType.fromJson(schema2JsonString).sql());

        //dft.withColumn("value", to_json(col("value"))).show(false);
        dft.select(from_json(col("value"), schema2).alias("json")).select("json.*").show();


        // from_json example
        //dft.withColumn("value", from_json(col("value"), schema2)).show(false);

        // json_tuple() example
        // query or extract the elements from JSON column and create the result as a new columns.

        dft.select(col("value"), json_tuple(col("value"), "Zipcode", "ZipCodeType", "City", "State"))
        .toDF( "Value", "Zipcode", "ZipCodeType", "City", "State")
        .show(false);

        // get_json_object() example
        // extract the JSON string based on path from the JSON column.
        dft.select(col("value"), get_json_object(col("value"), "$.ZipCodeType").alias("ZipCodeType")).show(false);

        // schema_of_json() example
        // create schema string from JSON string column.

        String schemaStr = spark.range(1)
            .select(schema_of_json(lit("{'Zipcode':704,'ZipCodeType':'STANDARD','City':'PARC PARQUE','State':'PR'}")))
            .collectAsList().get(0).getString(0);

        System.out.println(schemaStr);

    }
}
