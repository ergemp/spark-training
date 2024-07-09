package org.ergemp.sql.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDfFromCsvWithSql {
    public static void main(String[] args) {
        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromCsvWithSql")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> df = spark.sql("select * from csv.`file:///Users/ergemp/IdeaProjects/project_other/spark-training/data/deneme100.csv` " );
        //Dataset<Row> df = spark.sql("select * from csv.`hdfs:///csv/file/dir/file.csv` " );

        df.printSchema();
        df.show(100,false);
    }
}
