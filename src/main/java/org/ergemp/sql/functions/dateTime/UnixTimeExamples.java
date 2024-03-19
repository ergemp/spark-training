package org.ergemp.sql.functions.dateTime;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class UnixTimeExamples {
    public static void main(String[] args) {

        // https://sparkbyexamples.com/pyspark/pyspark-sql-working-with-unix-time-timestamp/

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("AddIntervalToDate")
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .getOrCreate();

        StructType structType = new StructType();
        structType = structType.add("col", DataTypes.StringType, false);

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create("2019-07-01 12:01:19.101"));
        nums.add(RowFactory.create("2019-06-24 12:01:19.222"));
        nums.add(RowFactory.create("2019-11-16 16:44:55.406"));
        nums.add(RowFactory.create("2019-11-16 16:50:59.406"));

        Dataset<Row> df = spark.createDataFrame(nums, structType);
        df.createOrReplaceTempView("df");

        df.show(false);

        Dataset<Row> df2 = df.select(
                unix_timestamp(col("col")).alias("timestamp_1"),
                unix_timestamp(col("col"),"MM-dd-yyyy HH:mm:ss").alias("timestamp_2"),
                unix_timestamp(col("col"),"MM-dd-yyyy").alias("timestamp_3"),
                unix_timestamp().alias("timestamp_4")
        );

        df2.show();

        df2.select(
                from_unixtime(col("timestamp_1")).alias("timestamp_1"),
                from_unixtime(col("timestamp_2"),"MM-dd-yyyy HH:mm:ss").alias("timestamp_2"),
                from_unixtime(col("timestamp_3"),"MM-dd-yyyy").alias("timestamp_3"),
                from_unixtime(col("timestamp_4")).alias("timestamp_4")
        ).show(false);
    }
}
