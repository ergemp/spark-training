package org.ergemp.workshop.s3Examples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class WriteToMinio {
    public static void main(String[] args) {

        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromCsv")
                .master("local")
                .config("fs.s3a.endpoint", "http://127.0.0.1:9000")
                .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
                .config("fs.s3a.connection.timeout", 600000)
                .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                .config("fs.s3a.connection.ssl.enabled", "true")
                .config("fs.s3a.path.style.access", "true")
                .config("spark.sql.debug.maxToStringFields", "100")

                .config("spark.speculation","false")
                .config("spark.driver.allowMultipleContext", "true")
                .config("fs.s3a.fast.upload", "true")

                .getOrCreate();

        //spark.sql("drop table if exists test_table");
        spark.sql("create table test_table (id INT, name STRING, age INT) USING CSV ");

        spark.sql("insert into test_table (id, name, age) values (1,'ergem', 46)");

        Dataset<Row> df = spark.sql("select * from test_table");

        df.write().mode(SaveMode.Overwrite).csv("s3a://bucket1/test_table");
        //df.show();

    }
}
