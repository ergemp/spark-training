package org.ergemp.sql.structType;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class MapType {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("MapType")
                .getOrCreate();

        StructType hair = new StructType().add("hair", DataTypes.StringType, true);
        StructType eye = new StructType().add("eye", DataTypes.StringType, true);

        StructType props = new StructType()
                .add("hair", DataTypes.StringType, true)
                .add("eye", DataTypes.StringType, true);

        StructType schema = new StructType()
                .add("name","String",true)
                .add("props", DataTypes.createStructType(props.fields()))
                ;

        Dataset<Row> df = spark.read().schema(schema).json("data/mapType.json");
        df.printSchema();
        df.show();

        df.select(df.col("name"), df.col("props.eye"), df.col("props.hair")).show();
    }
}
