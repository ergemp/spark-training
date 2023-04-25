package org.ergemp.sql.dsl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

public class Transform {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Transform")
                .getOrCreate();

        StructType s = new StructType()
                .add("home",new StructType()
                                .add("a_number", DataTypes.IntegerType, true)
                                .add("a_string",DataTypes.StringType, true)
                                .add("array_a",DataTypes.createArrayType(
                                        new StructType()
                                                .add("array_b", DataTypes.createArrayType( new StructType()
                                                        .add("a",DataTypes.StringType, true)
                                                        .add("b", DataTypes.IntegerType, true)
                                                        , true))
                                                .add("struct_c", new StructType()
                                                        .add("a",DataTypes.DoubleType,true)
                                                        .add("b",DataTypes.DoubleType, true))
                                                .add("array_d",DataTypes.createArrayType(DataTypes.StringType), true)
                                , true)
                        ,true), true)
                ;
        Dataset<Row> df = spark.read().schema(s).option("multiline", "true").option("inferschema",true).json("data/transform.json");
        df.printSchema();
        df.show(false);

        df.select("home.array_a.array_d").show(10, false);
        df.select(col("home.array_a.struct_c.a").alias("struct_field_inside_arrayA")).show(10, false);

        // df.select("home.array_a.array_b.a").show(10, false);
        // Exception in thread "main" org.apache.spark.sql.AnalysisException:
        // cannot resolve 'home.`array_a`.`array_b`['a']' due to data type mismatch:
        // argument 2 requires integral type, however, ''a'' is of string type.;

        // What I expect is a two-dimension array of string ([["1", "3"]] is my sample JSON)


        df.selectExpr("flatten(transform(home.array_a.array_b, x -> x.a)) as array_field_inside_array").show(false);


        // ref: https://stackoverflow.com/questions/57285628/spark-error-when-selecting-a-column-from-a-struct-in-a-nested-array
        // ref: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-transform.html
    }
}
