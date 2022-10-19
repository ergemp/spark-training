package org.ergemp.sql.dsl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.sparkproject.guava.collect.ImmutableList;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;

public class Explode {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Explode")
                .getOrCreate();

        Row r1 = RowFactory.create("fox brown fox jumped over the lazy dog");
        List<Row> rowList = ImmutableList.of(r1);

        StructType schemata = DataTypes.createStructType(
                new StructField[]{
                        createStructField("col1", StringType, false)
                });

        Dataset<Row> df = spark.createDataFrame(rowList, schemata);
        df.show();

        df
                .select(functions.explode(functions.split(col("col1")," ")).alias("words"))
                .groupBy("words")
                .count()
                .show();

    }
}
