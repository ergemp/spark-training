package org.ergemp.rdd.transformations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

public class FilterWithCustomFunction2 {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("FilterWithCustomFunction2")
                .master("local")
                .getOrCreate();

        JavaRDD<String> lines = null;
        lines = spark.read().textFile("hdfs://localhost:8020//mockdata/flumeData_181230_2000.1546189200983").toJavaRDD();

        Function<String, Boolean> filter = k -> (k.toString().indexOf("cookieGender") >= 0);

        JavaRDD<String> linesFiltered = lines.filter(filter);

        linesFiltered.take(10).forEach(item -> {
            if (item.matches(".*cookieGender.*")) {
                System.out.println(item);
            }
            else {
                System.out.println(item);
                //System.out.println("-----regex doesnt match");
            }
        });
    }
}
