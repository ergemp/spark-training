package org.ergemp.sql.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class CreateDfFromText2 {
    public static void main(String[] args) {
        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromText2")
                .config("spark.some.config.option", "some-value")
                .config("spark.sql.warehouse.dir","spark-warehouse/CreateDfFromText2")
                .config("hive.metastore.warehouse.dir","hive-warehouse/CreateDfFromText2")
                .master("local[*]")
                .getOrCreate();

        Dataset<String> textDF = spark.read().textFile("data/mock_clickStream.json");
        textDF.printSchema();
        /*
        * root
            |-- value: string (nullable = true)
        * */

        textDF.show();
        /*
        +--------------------+
        |               value|
        +--------------------+
        |{"sid":"2f662532-...|
        |{"sid":"6a5dba20-...|
        |{"sid":"75a86b28-...|
        |{"sid":"dd392bdc-...|
        |{"sid":"10304946-...|
        +--------------------+
        * */

        Long lCount = textDF.count();
        System.out.println(lCount);
        //5
    }
}
