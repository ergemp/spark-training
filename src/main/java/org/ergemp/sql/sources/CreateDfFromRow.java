package org.ergemp.sql.sources;

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

public class CreateDfFromRow {
    public static void main(String[] args) {

        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromRow")
                .master("local[2]")
                .getOrCreate();

        StructType structType = new StructType()
                .add("col1", DataTypes.StringType, false);

        Row myRow = RowFactory.create("col1");

        List<Row> nums = new ArrayList<Row>();
        nums.add(myRow);

        Dataset<Row> df = spark.createDataFrame(nums, structType);

        df.show();

        // ref: https://stackoverflow.com/questions/64931330/how-to-create-a-dataframe-using-spark-java
    }
}
