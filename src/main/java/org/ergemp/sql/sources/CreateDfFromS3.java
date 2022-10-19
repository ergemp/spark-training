package org.ergemp.sql.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDfFromS3 {
    public static void main(String[] args) {
        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf();
        conf.setAppName("CreateDfFromS3").setMaster("local[*]");

        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromS3")
                .master("local[*]")
                .config("spark.some.config.option", "some-value")
                .config("spark.hadoop.fs.s3a.access.key", "access.key")
                .config("spark.hadoop.fs.s3a.secret.key", "secret.key")
                .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
                .config("spark.speculation","false")
                .config("spark.driver.allowMultipleContext", "true")
                .config("fs.s3a.fast.upload", "true")
                .getOrCreate();

        Dataset<Row> df = spark.read().text("s3a://bucket_name/");
        df.createOrReplaceTempView("rawEvents");

        Dataset<Row> sqlDF = spark.sql("select count(*) from rawEvents");
        sqlDF.show();

        //read.json
        //start:    19:15:25
        //end:      19:32:44

        //read.text
        //start:    19:41:35
        //end:      19:50:50
    }
}
