package org.ergemp.workshop.s3Examples.interopWithTrino;

import org.apache.spark.sql.SparkSession;

public class CreateHiveTable {
    public static void main(String[] args) {


        // https://medium.com/@wasiualhasib/fixing-invalid-method-name-get-table-in-spark-when-connecting-to-hive-metastore-a1be6a618e2c

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .master("local")
                .config("spark.sql.warehouse.dir", "s3a://warehouse")

                //.config("hive.metastore.uris","thrift://localhost:9083")
                .config("spark.sql.catalogImplementation", "hive")
                .config("spark.sql.hive.metastore.version", "2.3.9")
                //.config("spark.sql.hive.metastore.jars", "/path/to/hive_jars239/*")
                .config("spark.hadoop.javax.jdo.option.ConnectionURL", "jdbc:postgresql://localhost:5432/hive")
                .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "hive")
                .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "hive")


                .config("hive.metastore.warehouse.dir","s3a://warehouse")
                .config("fs.s3a.endpoint", "http://localhost:9000")
                .config("fs.s3a.connection.timeout", 6000)
                .config("fs.s3a.access.key", "minioadmin")
                .config("fs.s3a.secret.key", "minioadmin")
                //.config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("show databases").show();
        spark.sql("show tables from default").show();
        spark.sql("select * from default.csv_table");
        //spark.sql("CREATE TABLE IF NOT EXISTS hive.src (key INT, value STRING) USING hive");


    }
}
