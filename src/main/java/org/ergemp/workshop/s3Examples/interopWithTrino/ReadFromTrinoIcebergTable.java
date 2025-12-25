package org.ergemp.workshop.s3Examples.interopWithTrino;

import org.apache.spark.sql.SparkSession;

public class ReadFromTrinoIcebergTable {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("SQLCreateIcebergTableExample")

                //.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

                //.config("spark.sql.catalog.iceberg","org.apache.iceberg.spark.SparkCatalog")
                //.config("spark.sql.catalog.iceberg.type","hadoop")
                    //.config("spark.sql.catalog.iceberg.type","hive")
                    //.config("spark.sql.catalog.iceberg.uri","thrift://localhost:9083")
                //.config("spark.sql.catalog.iceberg.warehouse","s3a://warehouse/")

                //.config("spark.sql.catalog.hive","org.apache.iceberg.spark.SparkCatalog")
                //.config("spark.sql.catalog.iceberg.hive","hadoop")
                    //.config("spark.sql.catalog.iceberg.type","hive")
                //.config("spark.sql.catalog.hive.uri","thrift://localhost:9083")
                //.config("spark.sql.catalog.hive.warehouse","s3a://warehouse/")

                .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083")

                    //.config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                    //.config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.hive.HiveCatalog")
                    //.config("spark.sql.catalog.demo.s3.endpoint", "http://localhost:9000")
                    //.config("spark.sql.defaultCatalog", "iceberg")

                //.config("spark.sql.catalogImplementation", "in-memory")
                //.config("spark.executor.heartbeatInterval", "3000")
                //.config("spark.network.timeout", "4000")

                .config("spark.sql.warehouse.dir","s3a://warehouse/")
                //.config("spark.sql.legacy.createHiveTableByDefault","true")

                .config("fs.s3a.endpoint", "http://localhost:9000")
                .config("fs.s3a.connection.timeout", 6000)
                .config("fs.s3a.access.key", "minioadmin")
                .config("fs.s3a.secret.key", "minioadmin")
                .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                .config("fs.s3a.path.style.access", "true")
                .config("fs.s3a.attempts.maximum", "1")
                .config("fs.s3a.connection.establish.timeout", "5000")
                .config("fs.s3a.connection.timeout", "10000")

                .enableHiveSupport()
                .getOrCreate();

        spark.sql("show databases").show();
        spark.sql("show schemas").show();
        spark.sql("show tables from default").show();
        spark.sql("show tables").show();

        //spark.sql("select * from iceberg.example_table2").show();
        spark.sql("select * from hive.csv_table").show();

        // read csv table created by trino
        spark.read().format("csv").load("S3a://warehouse/csv_table/").show(false);
        /*
        spark.read()
            .format("iceberg")
            .table("iceberg.example_table2")
            .show(false);
        */

    }
}
