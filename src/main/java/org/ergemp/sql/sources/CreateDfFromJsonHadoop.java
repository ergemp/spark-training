package org.ergemp.sql.sources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class CreateDfFromJsonHadoop {
    public static void main(String[] args) {
        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromJsonHadoop")
                .master("local")
                .getOrCreate();

        StructType schema = new StructType()
                .add("event", "string")
                .add("channel", "string")
                .add("cookieGender", "string");

        Dataset<Row> dataset1 = spark
                .read()
                .option("mode", "DROPMALFORMED")
                .schema(schema)
                .json("hdfs:///delphoi/delphoi-events-json/NewSession/year=2018/month=09/day=25/hour=15");
        dataset1.printSchema();
    }
}
