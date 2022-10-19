package org.ergemp.rdd.transformations;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

public class FilterWithRegex {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("rdd-regex-filter2")
                .master("local[1]")
                .getOrCreate();

        JavaRDD<String> lines = null;
        lines = spark.read().textFile("hdfs://localhost:8020//mockdata/flumeData_181230_2000.1546189200983").toJavaRDD();

        //Function<String, Boolean> filter = k -> (k.toString().matches(".*[cC]ookie[gG]ender.:.[mM].*"));
        //JavaRDD<String> linesFiltered = lines.filter(filter);

        JavaRDD<String> linesFiltered = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.matches(".*[cC]ookie[gG]ender.:.[mM].*");
            }
        });

        linesFiltered.take(10).forEach(item -> {
            System.out.println(item);
        });

        System.out.println("***** stopping and closing spark context");
        spark.stop();
        spark.close();
        System.out.println("***** stopped and closed spark context");

        //System.exit(0);
    }
}
