package org.ergemp.sql.sources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class CreateDfFromRow2 {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromRow2")
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

        spark.sql("select cast(null as string) as col1").show(false);

    }
}
