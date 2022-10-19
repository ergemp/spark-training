package org.ergemp.sql.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDfFromJdbc {
    public static void main(String[] args) {
        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromJdbc")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost/postgres")
                .option("dbtable", "pg_user")
                .option("user", "postgres")
                .option("password", "postgres")
                .load();

        jdbcDF.show(100,false);
    }
}
