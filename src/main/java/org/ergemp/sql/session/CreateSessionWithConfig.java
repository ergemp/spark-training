package org.ergemp.sql.session;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class CreateSessionWithConfig {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("CreateSessionWithConfig")
                .setMaster("local[*]");

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
    }
}
