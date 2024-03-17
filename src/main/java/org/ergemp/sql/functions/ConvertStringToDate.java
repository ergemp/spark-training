package org.ergemp.sql.functions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ConvertStringToDate {
    public static void main(String[] args) {

        // ref: https://stackoverflow.com/questions/64931330/how-to-create-a-dataframe-using-spark-java
        // ref: https://stackoverflow.com/questions/40763796/convert-date-from-string-to-date-format-in-dataframes
        // ref: https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("ConvertStringToDate")
                .getOrCreate();

        StructType structType = new StructType();
        structType = structType.add("A", DataTypes.StringType, false);

        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create((formatter.format(new Date()))));

        Dataset<Row> df = spark.createDataFrame(nums, structType);
        df.createOrReplaceTempView("df");

        //df.show(false);

        spark.sql("select * from df").show(false);
        spark.sql("select unix_timestamp(A, 'dd/MM/yyyy HH:mm:ss') from df").show(false);
        spark.sql("select to_date(cast(unix_timestamp(A, 'dd/MM/yyyy HH:mm:ss') as timestamp)) from df").show(false);

        spark.sql("SELECT TO_DATE(CAST(UNIX_TIMESTAMP('08/26/2016', 'MM/dd/yyyy') AS TIMESTAMP)) AS newdate").show(false);

        df
                .withColumn("formatted_date", org.apache.spark.sql.functions.to_date(functions.col("A"),"dd/MM/yyyy HH:mm:ss"))
                .show(false);

    }
}
