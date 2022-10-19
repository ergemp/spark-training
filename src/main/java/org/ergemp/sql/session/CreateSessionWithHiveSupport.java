package org.ergemp.sql.session;

import org.apache.spark.sql.SparkSession;

public class CreateSessionWithHiveSupport {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("CreateSessionWithHiveSupport")
                //.master("local")
                //.master("spark://phadoop01:7077")
                //.config("spark.cores.max",4)
                //.config("spark.executor.cores",2)
                //.config("spark.executor.memory","4g")
                //.config("dfs.nameservices", "heCluster01")
                //.config("hive.metastore.uris", "thrift://10.141.1.181:9083")
                .enableHiveSupport()
                .getOrCreate();
    }
}
