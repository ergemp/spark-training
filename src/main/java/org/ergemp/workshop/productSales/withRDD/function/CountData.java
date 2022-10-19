package org.ergemp.workshop.productSales.withRDD.function;

import org.apache.spark.api.java.JavaPairRDD;

import java.util.Map;

public class CountData {
    public static Map<String, Long> count(JavaPairRDD<String, String> d){
        Map<String, Long> result = d.countByKey();
        return result;
    }
}
