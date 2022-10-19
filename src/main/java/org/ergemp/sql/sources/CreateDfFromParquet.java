package org.ergemp.sql.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDfFromParquet {
    public static void main(String[] args) {
        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromParquet")
                .master("local[2]")
                .getOrCreate();

        String parquetPath = "data/part-00017.snappy.parquet" ;
        Dataset<Row> df = spark.read().parquet(parquetPath);

        df.createOrReplaceTempView("clickstream");

        df.printSchema();
        df
                .select("id","name" )
                .where("name != 'visit' ")
                .distinct()
                .show();

        spark
                .sql("select count(1) as total, name from clickstream group by name")
                .show(100, false);
    }
}
