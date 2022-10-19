package org.ergemp.sql.colAndRowExamples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.sparkproject.guava.collect.ImmutableList;

import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

public class CreateSparkRow {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("CreateSparkCol")
                .getOrCreate();

        Row r1 = RowFactory.create("name1", "value1", 1);
        Row r2 = RowFactory.create("name2", "value2", 2);
        List<Row> rowList = ImmutableList.of(r1, r2);

        StructType schemata = DataTypes.createStructType(
                new StructField[]{
                        createStructField("NAME", StringType, false),
                        createStructField("STRING_VALUE", StringType, false),
                        createStructField("NUM_VALUE", IntegerType, false),
                });

        Dataset<Row> data = spark.createDataFrame(rowList, schemata);
        data.show();
    }
}

// ref: https://stackoverflow.com/questions/39696403/how-to-create-a-row-from-a-list-or-array-in-spark-using-java