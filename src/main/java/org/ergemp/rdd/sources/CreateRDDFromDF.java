package org.ergemp.rdd.sources;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class CreateRDDFromDF {
    public static void main(String[] args) {
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreateRDDFromDF")
                .master("local[*]")
                .getOrCreate();

        StructType schema = new StructType()
                .add("event", "string")
                .add("channel", "string")
                .add("cookieGender", "string");

        JavaRDD<Row> items = null;
        items = spark.read().schema(schema).option("mode", "DROPMALFORMED").json("hdfs://localhost:8020/mockdata/flumeData_181230_2000.1546189200983").toJavaRDD();
        //spark.read().option("mode", "DROPMALFORMED").json("hdfs:///delphoi/delphoi-events-json/NewSession/year=2018/month=09/day=25/hour=15").schema().printTreeString();

        items.take(10).forEach(item -> {
            System.out.println(item);
        });
    }
}
