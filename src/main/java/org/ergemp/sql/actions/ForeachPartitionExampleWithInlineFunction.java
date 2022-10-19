package org.ergemp.sql.actions;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;

public class ForeachPartitionExampleWithInlineFunction {
    public static void main(String[] args){

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("ForeachPartitionExampleWithInlineFunction")
                .getOrCreate();

        Dataset<Row> df = spark.read().csv("data/deneme100.csv");

        df.foreachPartition(new ForeachPartitionFunction<Row>() {
            public void call(Iterator<Row> t) throws Exception {
                while (t.hasNext()){
                    Row row = t.next();
                    System.out.println(row.get(0).toString());
                }
            }
        });
    }
}


