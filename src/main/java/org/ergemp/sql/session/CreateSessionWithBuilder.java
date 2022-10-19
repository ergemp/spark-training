package org.ergemp.sql.session;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class CreateSessionWithBuilder {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("CreateSessionWithBuilder")
                .getOrCreate();
    }
}
