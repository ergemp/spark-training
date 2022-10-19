package org.ergemp.workshop.productSales.withRDD.function;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import org.apache.spark.api.java.Optional;


public class ModifyData {

    public static JavaPairRDD<String, String> modify(JavaRDD<Tuple2<Integer, Optional<String>>> d){
        return d.mapToPair(KEY_VALUE_PARSER.KEY_VALUE_PARSER);
    }
}
