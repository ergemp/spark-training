package org.ergemp.sql.functions.dateTime;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

public class AddIntervalToDate {
    public static void main(String[] args) {

        // ref: https://sparkbyexamples.com/spark/spark-add-hours-minutes-and-seconds-to-timestamp/

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("AddIntervalToDate")
                .getOrCreate();

        StructType structType = new StructType();
        structType = structType.add("col", DataTypes.StringType, false);

        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create("2019-07-01 12:01:19.101"));
        nums.add(RowFactory.create("2019-06-24 12:01:19.222"));
        nums.add(RowFactory.create("2019-11-16 16:44:55.406"));
        nums.add(RowFactory.create("2019-11-16 16:50:59.406"));

        Dataset<Row> df = spark.createDataFrame(nums, structType);
        df.createOrReplaceTempView("df");

        df.show();

        // example 1
        spark.sql("select col, " +
                "cast(col as TIMESTAMP) + INTERVAL 2 hours as added_hours," +
                "cast(col as TIMESTAMP) + INTERVAL 5 minutes as added_minutes," +
                "cast(col as TIMESTAMP) + INTERVAL 55 seconds as added_seconds from df"
        ).show(false);

        // example 2
        spark.sql( "select current_timestamp," +
                "cast(current_timestamp as TIMESTAMP) + INTERVAL 2 hours as added_hours," +
                "cast(current_timestamp as TIMESTAMP) + INTERVAL 5 minutes as added_minutes," +
                "cast(current_timestamp as TIMESTAMP) + INTERVAL 55 seconds as added_seconds"
        ).show(false);

        // example 3
        df.withColumn("added_hours", expr(" col + INTERVAL 2 HOURS"))
                .withColumn("added_minutes", expr("col + INTERVAL 2 minutes"))
                .withColumn("added_seconds",expr("col + INTERVAL 2 seconds"))
                .show(false);

        // example 4
        df.withColumn("minus_hours", expr(" col - INTERVAL 2 HOURS"))
                .withColumn("minus_minutes", expr("col - INTERVAL 2 minutes"))
                .withColumn("minus_seconds",expr("col - INTERVAL 2 seconds"))
                .show(false);
    }
}
