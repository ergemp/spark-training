package org.ergemp.sql.structuredStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.expressions.javalang.typed;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.ergemp.sql.structuredStreaming.model.DeviceData;

import java.util.concurrent.TimeoutException;

public class BasicOperationsExample {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .appName("BasicOperationsExample")
                .master("local")
                .getOrCreate();

        // Read all the csv files written atomically in a directory
        StructType schema = new StructType()
                .add("device", DataTypes.StringType)
                .add("type", DataTypes.StringType)
                .add("signal", DataTypes.DoubleType)
                .add("time", DataTypes.DateType)
                ;

        Dataset<Row> df = spark
                .readStream()
                .schema(schema)
                .json("data/streaming/BasicOperationsExample/");    // Equivalent to format("csv").load("/path/to/directory")

        //df.writeStream().format("console").start();

        Dataset<DeviceData> ds = df.as(ExpressionEncoder.javaBean(DeviceData.class));

        //
        // Select the devices which have signal more than 10
        //
        StreamingQuery query =
                df.select("device").where("signal > 10")
                .writeStream()
                .format("console")
                .start();

        ds.filter((FilterFunction<DeviceData>) value -> value.getSignal() > 10)
                .map((MapFunction<DeviceData, String>) value -> value.getDevice(), Encoders.STRING());

        // Running count of the number of updates for each device type
        df.groupBy("type").count(); // using untyped API

        // Running average signal for each device type
        ds
                .groupByKey((MapFunction<DeviceData, String>) value -> value.getType(), Encoders.STRING())
                .agg(typed.avg((MapFunction<DeviceData, Double>) value -> value.getSignal()));

        df.createOrReplaceTempView("updates");
        spark.sql("select count(*) from updates");  // returns another streaming DF

        query.awaitTermination();

    }
}
