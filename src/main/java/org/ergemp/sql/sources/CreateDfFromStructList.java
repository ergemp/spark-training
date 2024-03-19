package org.ergemp.sql.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CreateDfFromStructList {
    public static void main(String[] args) {

        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromStructList")
                .master("local")
                .getOrCreate();

        StructType structType = new StructType();
        structType = structType.add("A", DataTypes.StringType, false);
        structType = structType.add("B", DataTypes.StringType, false);

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create("value1", "value2"));
        nums.add(RowFactory.create("value3", "value4"));

        Dataset<Row> df = spark.createDataFrame(nums, structType);

        df.show(false);

    }
}
