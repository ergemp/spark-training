package org.ergemp.rdd.transformations.pairRdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class ReduceByKeyExample2 {
    public static void main(String[] args) {
        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("ReduceByKeyExample")
                .master("local[1]")
                .getOrCreate();

        JavaRDD<String> lines = null;
        lines = spark.read().textFile("hdfs://localhost:8020//mockdata/flumeData_181230_2000.1546189200983").toJavaRDD();

        Function<String, Boolean> filter = k -> (k.toString().matches(".*[cC]ookie[gG]ender.:.[mMfF].*"));
        JavaRDD<String> linesFiltered = lines.filter(filter);

        JavaRDD<String> linesMapped = linesFiltered.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return v1.substring(v1.indexOf("cookieGender") + "cookieGender".length() + 3, v1.indexOf("cookieGender") + "cookieGender".length() + 3 + 1);
            }
        });

        JavaPairRDD<String, Integer> pairedLines = linesMapped.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> countLines = pairedLines.reduceByKey((a, b) -> a + b);

        countLines.take(10).forEach(item -> {
            System.out.println(item);
            /*
            (M,335)
            (F,1154)
            */
        });

        System.out.println("***** stopping and closing spark session");
        spark.stop();
        spark.close();
        System.out.println("***** stopped and closed spark session");

        //System.exit(0);
        /*
        (M,7083)
        (F,33414)
        */
    }
}
