package org.ergemp.udfExamples;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class ScalarExample {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("org.ergemp.udfExamples.scalarExample")
                .master("local")
                .getOrCreate();

        ScalarFunctions functions = new ScalarFunctions();

        functions.random.asNondeterministic();

        // register predefined UDF
        spark.udf().register("random", functions.random);
        spark.sql("SELECT random()").show();

        // Define and register a one-argument UDF
        spark.udf().register("plusOne",
                (UDF1<Integer, Integer>) x -> x + 1, DataTypes.IntegerType);

        spark.sql("SELECT plusOne(5)").show();


        spark.udf().register("strLen", functions.strLen);
        spark.sql("SELECT strLen('ttt',1)").show();

        spark.udf().register("strLen2", functions.strLen2);
        spark.sql("SELECT strLen2('ttt',1)").show();


        // UDF in a WHERE clause
        spark.udf().register("oneArgFilter",
                (UDF1<Long, Boolean>) x -> x > 5, DataTypes.BooleanType);
        spark.range(1, 10).createOrReplaceTempView("test");
        spark.sql("SELECT * FROM test WHERE oneArgFilter(id)").show();


        spark.udf().register("capitalA", functions.capitalA);
        spark.sql("SELECT capitalA('aaa')").show();



    }
}
