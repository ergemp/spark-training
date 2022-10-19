package org.ergemp.sql.structType;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.explode;

public class ArrayType {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("ArrayType")
                .getOrCreate();

        StructType productList = new StructType().add("productIds",DataTypes.StringType);

        StructType schema = new StructType()
                .add("sid","Integer",true)
                .add("pid","String")
                .add("userId","String")
                .add("ts","String")
                .add("event","String")
                //.add("productList", DataTypes.createArrayType(DataTypes.createStructType(Arrays.asList(new StructField())),true))
                .add("productList", DataTypes.createArrayType(productList,true))
                //.add("productList", DataTypes.createArrayType(DataTypes.StringType,true))
                ;

        Dataset<Row> df = spark.read().schema(schema).json("data/mock_clickStream.json");
        df.show();

        df.select(df.col("pid"), explode(df.col("productList")).alias("productIds")).groupBy("pid").count().orderBy().show();
    }
}
