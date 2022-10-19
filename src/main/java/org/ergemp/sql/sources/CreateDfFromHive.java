package org.ergemp.sql.sources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class CreateDfFromHive {
    public static void main(String[] args) {
        try {
            String sparkWarehouseLocation = new File("spark-warehouse/CreateDfFromHive").getAbsolutePath();
            String hiveWarehouseLocation = new File("hive-warehouse/CreateDfFromHive").getAbsolutePath();

            SparkSession spark = SparkSession
                    .builder()
                    .appName("CreateDfFromHive")
                    .config("spark.sql.warehouse.dir", sparkWarehouseLocation)
                    .config("hive.metastore.warehouse.dir", hiveWarehouseLocation)
                    //.config("hive.metastore.uris", "thrift://localhost:9083")
                    .enableHiveSupport()
                    .getOrCreate();

            spark.sql("show tables").show();

            /*
            /u01/hadoop/spark/bin/spark-submit --class org.ergemp.sql.sources.CreateDfFromHive --master local spark_training-1.0-SNAPSHOT-jar-with-dependencies.jar
            +--------+--------------------+-----------+
            |database|           tableName|isTemporary|
            +--------+--------------------+-----------+
            | default|      accatalognodes|      false|
            | default|        accategories|      false|
            | default|        acorderitems|      false|
            | default|       acorderitems2|      false|
            | default|            acorders|      false|
            | default|     boutiquecounter|      false|
            | default|      boutiquedetail|      false|
            | default|boutiquedetailimp...|      false|
            | default|boutiquedetailimp...|      false|
            | default| boutiquedetail_json|      false|
            | default|boutiqueimpressio...|      false|
            | default|      mdboutiqueplan|      false|
            | default|          newsession|      false|
            | default|     newsession_json|      false|
            | default|        ordersummary|      false|
            | default|   ordersummary_json|      false|
            | default|     product_content|      false|
            | default|         productview|      false|
            | default|    productview_json|      false|
            | default|                  tt|      false|
            +--------+--------------------+-----------+
            */

            Dataset<Row> sqlDF = spark.sql("show tables");
            sqlDF.show();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        finally {
        }
    }
}
