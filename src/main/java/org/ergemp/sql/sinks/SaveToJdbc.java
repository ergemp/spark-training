package org.ergemp.sql.sinks;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SaveToJdbc {
    public static void main(String[] args) {
        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("SaveToJdbc")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true") //first line in file has headers
                .option("mode", "DROPMALFORMED")
                .load("data/test.csv");

        Properties cnnProps = new Properties();
        cnnProps.setProperty("driver", "org.postgresql.Driver");
        cnnProps.setProperty("user", "postgres");
        cnnProps.setProperty("password", "password");

        df.write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost/postgres","spark_table", cnnProps);
        //Dataset<Row> df2 = spark.sql("SELECT * FROM csv.'hdfs:///csv/file/dir/file.csv'");
    }
}
