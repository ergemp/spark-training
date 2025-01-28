package org.ergemp.udfExamples;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.udf;

public class ScalarFunctions {

    // ref: https://spark.apache.org/docs/latest/sql-ref-functions-udf-scalar.html

    // Define a zero-argument non-deterministic UDF
    // UDF is deterministic by default, i.e. produces the same result for the same input.
    UserDefinedFunction random = udf(
            () -> Math.random(), DataTypes.DoubleType
    );

    // Define one-argument UDF
    UserDefinedFunction plusOne = udf(
            (UDF1<Integer, Integer>) x -> x + 1, DataTypes.IntegerType
    );

    // Define two-argument UDF
    UserDefinedFunction strLen = udf(
            (String s, Integer x) -> s.length() + x, DataTypes.IntegerType
    );

    UserDefinedFunction strLen2 = udf(
            (String s, Integer x) -> {
                return s.length() + x;
            }, DataTypes.IntegerType
    );


    UserDefinedFunction capitalA = udf(
            (UDF1<String, String>) s -> {
                return s.replace("a","A");
            }, DataTypes.StringType
    );



}
