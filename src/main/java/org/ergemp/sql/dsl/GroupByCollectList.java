package org.ergemp.sql.dsl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class GroupByCollectList {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("GroupByCollectList")
                .getOrCreate();

        Dataset<Row> df = spark.read().option("inferschema",true).json("data/groupByCollectList.json");
        df.printSchema();
        df.show(false);

        df.groupBy(col("name"),col("age"))
                .agg(
                        collect_list(to_json(struct("producta1"))).as("producta1"), // use to_json & struct functions here.
                        collect_list(to_json(struct("producta3"))).as("producta3") // use to_json & struct functions here.
                ).show(false);

        df.groupBy(col("name"),col("age"))
                .agg(
                        collect_list(col("producta1")).as("producta1"), // use to_json & struct functions here.
                        collect_list(col("producta3")).as("producta3") // use to_json & struct functions here.
                ).show(false);
    }

    // ref: https://stackoverflow.com/questions/71035422/convert-array-of-string-to-array-of-struct-in-spark-java
}
