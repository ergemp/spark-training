package org.ergemp.workshop.productSales.withRDD.function;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import org.apache.spark.api.java.Optional;

public class JoinData {
    public static JavaRDD<Tuple2<Integer, Optional<String>>> join(JavaPairRDD<Integer, Integer> s, JavaPairRDD<Integer, String> c) {
        JavaRDD<Tuple2<Integer,Optional<String>>> leftJoinOutput = s.leftOuterJoin(c).values().distinct();
        return leftJoinOutput;
    }
}
