package org.ergemp;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PartitionDiscovery {
    public static void main(String[] args) {
        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("PartitionDiscovery")
                .master("local")
                .getOrCreate();

        // ref: https://medium.com/@goyalarchana17/spark-series-partition-discovery-b2ef11d086ca
        // ref: https://medium.com/geekculture/finding-the-latest-date-is-not-as-easy-as-you-would-think-2d6a0a49eda1

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("delimiter","\t")
                .option("header", "true")
                .option("mode", "DROPMALFORMED")
                .option("basePath", "/path/to/basepath/")
                .load("/path/to/basepath/year=*");


        // The automatic partition discovery can be disabled by setting the
        // spark.sql.sources.partitionDiscovery.enabled configuration to false


        df.printSchema();
        df.show(100,false);
    }
}
