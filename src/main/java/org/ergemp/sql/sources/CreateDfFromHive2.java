package org.ergemp.sql.sources;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;

public class CreateDfFromHive2 {
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("CreateDfFromHive2")
                .config("spark.sql.warehouse.dir", "spark-warehouse/CreateDfFromHive2")
                .enableHiveSupport()
                .getOrCreate();

        spark.catalog().listDatabases().show(false);
        spark.catalog().listTables("test").show(false);

        spark.sqlContext().sql("use test");
        spark.sql("insert into table test_external_table select t.* from (select 1, 'a') t");

        spark.sql("select * from test_external_table").show();
    }
}
