package org.ergemp.workshop.s3Examples.icebergExamples2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UpdateIcebergExamplePartitioning {
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

        // Partition table based on "VendorID" column
        //Logger.getLogger("tt").info("Partitioning table based on VendorID column...");
        //spark.sql("ALTER TABLE iceberg.taxis_large ADD PARTITION FIELD VendorID");

        // Query Metadata tables like snapshot, files, history
        Logger.getLogger("tt").info("Querying Snapshot table...");
        Dataset<Row> snapshots_df = spark.sql("SELECT * FROM iceberg.taxis_large.snapshots ORDER BY committed_at");
        snapshots_df.show();  // shows all the snapshots in ascending order of committed_at column

        Logger.getLogger("tt").info("Querying Files table...");
        Dataset<Row> files_count_df = spark.sql("SELECT COUNT(*) AS cnt FROM iceberg.taxis_large.files");
        Long total_files_count = files_count_df.first().getLong(0);
        Logger.getLogger("tt").info("Total Data Files for NYC Taxi Data: " + total_files_count);

        spark.sql("SELECT file_path, " +
                    " file_format, " +
                    " record_count, " +
                    " null_value_counts, " +
                    " lower_bounds, "+
                    " upper_bounds " +
                    " FROM iceberg.taxis_large.files LIMIT 1").show();

        // Query history table
        Logger.getLogger("tt").info("Querying History table...");
        Dataset<Row> hist_df = spark.sql("SELECT * FROM iceberg.taxis_large.history");
        hist_df.show();

        // Time travel to initial snapshot
        Logger.getLogger("tt").info("Time Travel to initial snapshot...");
        Dataset<Row> snap_df = spark.sql("SELECT snapshot_id FROM iceberg.taxis_large.history LIMIT 1");
        // spark.sql("CALL demo.system.rollback_to_snapshot('iceberg.taxis_large', " + snap_df.first().getAs("snapshot_id").toString() + ")");

        // Query the table to see the results
        Dataset<Row> res_df = spark.sql("SELECT VendorID " +
                    " ,tpep_pickup_datetime " +
                    " ,tpep_dropoff_datetime " +
                    " ,fare " +
                    " ,distance " +
                    " ,fare_per_distance " +
                    " FROM iceberg.taxis_large LIMIT 15");

        res_df.show();

        // Query history table
        Logger.getLogger("tt").info("Querying History table...");
        hist_df = spark.sql("SELECT * FROM iceberg.taxis_large.history");
        hist_df.show();  // 1 new row

        // Query table row count
        Dataset<Row> count_df = spark.sql("SELECT COUNT(*) AS cnt FROM iceberg.taxis_large");
        Long total_rows_count = count_df.first().getLong(0);
        Logger.getLogger("tt").info("Total Rows for NYC Taxi Data after time travel: " + total_rows_count);

    }
}
