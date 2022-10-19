package org.ergemp.sql.session;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class CreateSessionFromContext {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("CreateSessionFromContext")
                .setMaster("local[1]");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        SparkSession sparkSession = new SparkSession(jsc.sc());

        sparkSession.sql("select 1");
    }
}
