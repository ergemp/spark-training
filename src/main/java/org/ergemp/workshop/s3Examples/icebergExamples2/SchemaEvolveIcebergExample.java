package org.ergemp.workshop.s3Examples.icebergExamples2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SchemaEvolveIcebergExample {
    public static void main(String[] args) throws AnalysisException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("SQLCreateIcebergTableExample")

                //.config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.8.1') \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalogImplementation", "in-memory")

                //.config("spark.sql.catalog.iceberg.hive.uri","thrift://192.168.56.2:9083")
                .config("spark.sql.catalog.iceberg","org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.iceberg.type","hadoop")
                .config("spark.sql.catalog.iceberg.warehouse","s3a://warehouse/")
                //.config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                //.config("spark.sql.catalog.iceberg.s3.endpoint", "http://192.168.56.2:9000")
                //.config("spark.sql.defaultCatalog", "iceberg")

                .config("spark.executor.heartbeatInterval", "300000")
                .config("spark.network.timeout", "400000")

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

                .getOrCreate();

                //Dataset<Row> df = spark.read().option("header","true").csv("s3a://bucket1/2018_Yellow_Taxi_Trip_Data.csv");
                //df.write().mode("overwrite").saveAsTable("taxis_large");

                //spark.sql("show databases").show();
                spark.catalog().listDatabases().show();
                /*
                +-------+----------------+----------------+
                |   name|     description|     locationUri|
                +-------+----------------+----------------+
                |default|default database|s3a://warehouse/|
                +-------+----------------+----------------+
                */

                spark.catalog().listTables().show();
                /*
                +----+--------+-----------+---------+-----------+
                |name|database|description|tableType|isTemporary|
                +----+--------+-----------+---------+-----------+
                +----+--------+-----------+---------+-----------+
                */

                spark.sql("ALTER TABLE iceberg.taxis_large RENAME COLUMN fare_amount TO fare");
                spark.sql("ALTER TABLE iceberg.taxis_large RENAME COLUMN trip_distance TO distance");
                spark.sql("ALTER TABLE iceberg.taxis_large ALTER COLUMN distance COMMENT 'The elapsed trip distance in miles reported by the taximeter.'");

                spark.sql("ALTER TABLE iceberg.taxis_large ALTER COLUMN distance AFTER fare");
                spark.sql("ALTER TABLE iceberg.taxis_large ADD COLUMN fare_per_distance FLOAT AFTER distance");

                Dataset<Row> snap_df = spark.sql("SELECT * FROM iceberg.taxis_large.snapshots");
                snap_df.show();  // prints all the available snapshots (1 till now)

    }
}
