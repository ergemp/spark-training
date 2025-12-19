package org.ergemp.workshop.s3Examples;

import org.apache.spark.sql.SparkSession;

public class SQLSelectIcebergTableExample {
    public static void main(String[] args) {
        try {
            //String sparkWarehouseLocation = new File("spark-warehouse").getAbsolutePath();
            //String hiveWarehouseLocation = new File("hive-warehouse").getAbsolutePath();


            SparkSession spark = SparkSession
                    .builder()
                    .master("local")
                    .appName("SQLSelectIcebergTableExample")

                    .config("spark.sql.catalog.iceberg","org.apache.iceberg.spark.SparkCatalog")
                    .config("spark.sql.catalog.iceberg.type","hadoop")
                    .config("spark.sql.catalog.iceberg.warehouse","s3a://warehouse/")
                    //.config("spark.sql.catalog.hive.uri","thrift://localhost:9083")
                    .config("spark.sql.catalog.iceberg.warehouse.dir","s3a://warehouse/")
                    //.config("spark.sql.warehouse.dir","s3a://warehouse/")
                    //.config("spark.sql.legacy.createHiveTableByDefault","true")

                    .config("fs.s3a.endpoint", "http://127.0.0.1:9000")
                    .config("fs.s3a.connection.timeout", 600000)
                    .config("fs.s3a.access.key", "minioadmin")
                    .config("fs.s3a.secret.key", "minioadmin")
                    .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                    //.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

                    .getOrCreate();

            spark.sql("show databases").show();
            spark.sql("show schemas").show();
            spark.sql("show tables from default").show();
            spark.sql("show tables").show();

            //spark.sql("create table default.spark_table (col1 string) using json");
            spark.sql("select * from iceberg.spark_table").show();

            //spark.sql("drop table iceberg.spark_table purge").show();

        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        finally {
        }
    }
}
