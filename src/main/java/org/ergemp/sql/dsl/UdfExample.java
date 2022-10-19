package org.ergemp.sql.dsl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.immutable.Seq;

import static org.apache.spark.sql.functions.*;

public class UdfExample {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("UdfExample")
                .getOrCreate();

        // User-Defined Functions (UDFs) are user-programmable routines that act on one row.

        // Define and register a zero-argument non-deterministic UDF
        // UDF is deterministic by default, i.e. produces the same result for the same input.
        spark.udf().register("plusOne",
                (UDF1<Integer, Integer>) x -> x + 1, DataTypes.IntegerType);

        UserDefinedFunction random = udf(
                () -> Math.random(), DataTypes.DoubleType
        );

        random.asNondeterministic();
        spark.udf().register("random", random);

        spark.sql("SELECT random()").show();

        // Define and register a one-argument UDF
        spark.udf().register("plusOne",
                (UDF1<Integer, Integer>) x -> x + 1, DataTypes.IntegerType);

        spark.sql("SELECT plusOne(5)").show();

        // Define and register a two-argument UDF

        UserDefinedFunction strLen = udf(
                (String s, Integer x) -> s.length() + x, DataTypes.IntegerType
        );

        spark.udf().register("strLen", strLen);
        spark.sql("SELECT strLen('test', 1)").show();

        // UDF in a WHERE clause
        spark.udf().register("oneArgFilter",
                (UDF1<Long, Boolean>) x -> x > 5, DataTypes.BooleanType);

        spark.range(1, 10).createOrReplaceTempView("test");
        spark.sql("SELECT * FROM test WHERE oneArgFilter(id)").show();


        // spark >= 2.3
        UserDefinedFunction plusTwo = udf(
                (Long x) -> x + 2, DataTypes.LongType
        );
        spark.udf().register("plusTwo", plusTwo);

        Dataset<Row> df = spark.sql("select * from test");
        df.select(plusTwo.apply(col("id"))).show();

        //spark < 2.3
        UDF1 plusThree = new UDF1() {
            @Override
            public Object call(Object o) throws Exception {
                if (o instanceof Long) {
                    return (Long) o + 3;
                }
                else{
                    return null;
                }
            }
        };

        spark.sqlContext().udf().register("plusThree", plusThree, DataTypes.LongType);

        df.select(callUDF("plusThree", col("id"))).show();
        df.selectExpr("plusThree(id)").show();

    }
}

// ref: https://spark.apache.org/docs/latest/sql-ref-functions-udf-scalar.html
// ref: https://stackoverflow.com/questions/35348058/how-do-i-call-a-udf-on-a-spark-dataframe-using-java
// ref: https://spark.apache.org/docs/latest/sql-ref-functions-udf-hive.html

