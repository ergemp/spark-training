package org.ergemp.sql.sources;

import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.Seq;

import java.util.Arrays;

public class CreateDfFromJsonString {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("CreateDfFromJsonString")
                .getOrCreate();

        String jsonStr = "{'metadata':{'key':84896,'value':54}}" ;
        JavaRDD rdd = new JavaSparkContext(spark.sparkContext()).parallelize(Arrays.asList(jsonStr));

        Dataset<Row> df = spark.read().json(rdd).toDF();

        df.show();

    }
}
