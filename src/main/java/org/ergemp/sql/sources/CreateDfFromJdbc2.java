package org.ergemp.sql.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDfFromJdbc2 {
    public static void main(String[] args) {
        Logger log = Logger.getRootLogger();
        log.setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQLExample")
                .master("local")
                .getOrCreate();

        // read list to RDD
        String jsonPath = "resources/mock_clickStream.json";
        Dataset<Row> df = spark.read().json(jsonPath);

        df.printSchema();

        df.createOrReplaceTempView("df");
        spark.sql("select * from df where event='newSession' and ip_address like '2%'").show(false);

    }
}
