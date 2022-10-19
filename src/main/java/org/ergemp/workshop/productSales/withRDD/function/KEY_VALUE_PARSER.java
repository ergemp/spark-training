package org.ergemp.workshop.productSales.withRDD.function;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import org.apache.spark.api.java.Optional;

public class KEY_VALUE_PARSER {
    public static final PairFunction<Tuple2<Integer, Optional<String>>, String, String> KEY_VALUE_PARSER =
            new PairFunction<Tuple2<Integer, Optional<String>>, String, String>() {
                public Tuple2<String, String> call(
                        Tuple2<Integer, Optional<String>> a) throws Exception {
                    // a._2.isPresent()
                    return new Tuple2<String, String>(a._2.get(), a._2.get());
                }
            };
}
