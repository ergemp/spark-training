package org.ergemp.sql.functions.dateTime;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import javax.xml.crypto.Data;

import static org.apache.spark.sql.functions.*;

public class DateTimeFunctions {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("DateTimeFunctions")
                .getOrCreate();

        //StructType schema = new StructType().add("customer_id","Integer");
        //Dataset<Row> df = spark.read().schema(schema).json("data/customerSales/spark_training_customers.json");
        Dataset<Long> dft = spark.range(1);

        Dataset<Row> df = spark.read().option("inferschema",true).json("data/customerSales/spark_training_sales.json");
        df.printSchema();

        // create new column with epoch date
        Dataset<Row> df2 = df.withColumn("date", lit(System.currentTimeMillis()));
        df2.show(1);

        df.withColumn("current", lit(current_date())).show();

        // create current date
        df.select(current_date().alias("current_date")).show(1);

        // to_date()
        df
                .withColumn("dateString", lit("09-22-2022"))
                .select(col("dateString"),
                        to_date(col("dateString"), "MM-dd-yyyy").alias("to_date"))
                .show(1);

        // date_format()
        df
                .withColumn("dateString", lit("09-22-2022"))
                .select(col("dateString"),
                        to_date(col("dateString"), "MM-dd-yyyy").alias("to_date"),
                        date_format(to_date(col("dateString"), "MM-dd-yyyy"),"ddMMyyyy").alias("to_date"))
                .show(1);

        // from_unixtime()
        df.select(col("ts"), functions.from_unixtime(col("ts").$div(1000))).show(1);

        // unix_timestamp(), from_unixtime()
        df2
            .withColumn("unix_time", unix_timestamp())
            .withColumn("unix_time_conv", unix_timestamp(to_timestamp(from_unixtime(col("ts").$div(1000)))))
            .withColumn("timestamp_conv", from_unixtime(unix_timestamp(from_unixtime(col("ts").$div(1000)))))
            .show(1, false);

        // datediff()
        df.select(col("ts"), functions.from_unixtime(col("ts").$div(1000))).show(1);

        df.select(col("ts"), datediff(current_date(), from_unixtime(col("ts").$div(1000))).alias("datediff")).show();

        // months_between()
        df.select(col("ts"), months_between(current_date(), from_unixtime(col("ts").$div(1000))).alias("months_between")).show();

        /*
        trunc()
        Returns date truncated to the unit specified by the format.
        For example, `trunc("2018-11-19 12:01:19", "year")` returns 2018-01-01
        format: 'year', 'yyyy', 'yy' to truncate by year,
        'month', 'mon', 'mm' to truncate by month
        */
        df.select(col("ts"),
                trunc(from_unixtime(col("ts").$div(1000)), "Month").alias("Month_Trunc"),
                trunc(from_unixtime(col("ts").$div(1000)), "Year").alias("Month_Year"),
                trunc(from_unixtime(col("ts").$div(1000)), "Month").alias("Month_Trunc")
        ).show();

        /*
        date_trunc()

        Returns timestamp truncated to the unit specified by the format.
        For example, `date_trunc("year", "2018-11-19 12:01:19")` returns 2018-01-01 00:00:00
        format: 'year', 'yyyy', 'yy' to truncate by year,
        'month', 'mon', 'mm' to truncate by month,
        'day', 'dd' to truncate by day,
        Other options are: 'second', 'minute', 'hour', 'week', 'month', 'quarter'
        */

        // add_months() , date_add(), date_sub()
        df.select(col("ts"),
                from_unixtime(col("ts").$div(1000)),
                add_months(from_unixtime(col("ts").$div(1000)),3).alias("add_months"),
                add_months(from_unixtime(col("ts").$div(1000)),-3).alias("sub_months"),
                date_add(from_unixtime(col("ts").$div(1000)),4).alias("date_add"),
                date_sub(from_unixtime(col("ts").$div(1000)),4).alias("date_sub")
        ).show(1);

        // year(), month(), month(), next_day(), last_day(), weekofyear()
        df.select(col("ts"),
                year(from_unixtime(col("ts").$div(1000))).alias("year"),
                month(from_unixtime(col("ts").$div(1000))).alias("month"),
                next_day(from_unixtime(col("ts").$div(1000)), "Sunday").alias("next_day"),
                last_day(from_unixtime(col("ts").$div(1000))).alias("ladt_day"),
                weekofyear(from_unixtime(col("ts").$div(1000))).alias("weekofyear")
        ).show(1);

        // dayofweek(), dayofmonth(), dayofyear()
        df.select(from_unixtime(col("ts").$div(1000)),
                dayofweek(from_unixtime(col("ts").$div(1000))).alias("dayofweek"),
                dayofmonth(from_unixtime(col("ts").$div(1000))).alias("dayofmonth"),
                dayofyear(from_unixtime(col("ts").$div(1000))).alias("dayofyear")
                ).show(1);

        // timestamp functions
        df2.select(current_timestamp().alias("current_timestamp")).show(1, false);

        // to_timestamp()
        df2.select(col("ts"),
                from_unixtime(col("ts").$div(1000)),
                to_timestamp(from_unixtime(col("ts").$div(1000)), "yyy-MM-dd HH:mm:ss").alias("to_timestamp"))
                .show(1, false);

        df2.select(col("ts"),
                from_unixtime(col("ts").$div(1000)),
                hour(from_unixtime(col("ts").$div(1000))).alias("hour"),
                minute(from_unixtime(col("ts").$div(1000))).alias("minute"),
                second(from_unixtime(col("ts").$div(1000))).alias("second")
        ).show(1,false);


    }
}
