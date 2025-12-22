package org.ergemp.workshop.s3Examples.icebergExamples1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class DfCreateIcebergTableExample {
    public static void main(String[] args) {
        try {
            //String sparkWarehouseLocation = new File("spark-warehouse").getAbsolutePath();
            //String hiveWarehouseLocation = new File("hive-warehouse").getAbsolutePath();


            SparkSession spark = SparkSession
                    .builder()
                    .master("local")
                    .appName("SQLCreateIcebergTableExample")

                    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

                    .config("spark.sql.catalog.iceberg","org.apache.iceberg.spark.SparkCatalog")
                    .config("spark.sql.catalog.iceberg.type","hadoop")
                    .config("spark.sql.catalog.iceberg.warehouse","s3a://warehouse/")

                    //.config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                    //.config("spark.sql.catalog.demo.s3.endpoint", "http://192.168.56.2:9000")
                    //.config("spark.sql.defaultCatalog", "iceberg")

                    .config("spark.sql.catalogImplementation", "in-memory")
                    .config("spark.sql.catalog.demo.type", "hadoop")
                    .config("spark.executor.heartbeatInterval", "300000")
                    .config("spark.network.timeout", "400000")

                    //.config("spark.sql.catalog.hive.uri","thrift://localhost:9083")

                    .config("spark.sql.warehouse.dir","s3a://warehouse/")
                    //.config("spark.sql.legacy.createHiveTableByDefault","true")

                    .config("fs.s3a.endpoint", "http://192.168.56.2:9000")
                    .config("fs.s3a.connection.timeout", 600000)
                    .config("fs.s3a.access.key", "minioadmin")
                    .config("fs.s3a.secret.key", "minioadmin")
                    .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                    .config("fs.s3a.path.style.access", "true")
                    .config("fs.s3a.attempts.maximum", "1")
                    .config("fs.s3a.connection.establish.timeout", "5000")
                    .config("fs.s3a.connection.timeout", "10000")

                    //.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

                    .getOrCreate();



            StructType schema = new StructType()
                    .add("id", "integer")
                    .add("name", "string")
                    .add("value", "integer")
                    ;


            List<Row> data = new ArrayList<Row>();
            data.add(RowFactory.create(1,"metric_1",5));
            data.add(RowFactory.create(2,"metric_2",10));
            data.add(RowFactory.create(3,"metric_1",5));
            data.add(RowFactory.create(4,"metric_2",10));
            data.add(RowFactory.create(5,"metric_1",5));


            Dataset<Row> df = spark.createDataFrame(data, schema);


            df.writeTo("iceberg.datapoints").using("iceberg").create();


        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        finally {
        }
    }
}
