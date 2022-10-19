package org.ergemp.rdd.sources;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CreateRDDFromS3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("S3Source").setMaster("local[*]");
        conf.set("spark.hadoop.fs.s3a.access.key", "access.key");
        conf.set("spark.hadoop.fs.s3a.secret.key", "secret.key");
        conf.set("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2");
        conf.set("spark.speculation","false");
        conf.set("spark.driver.allowMultipleContext", "true");
        conf.set("fs.s3a.fast.upload", "true");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> s3Data = jsc.textFile("s3a://bucket_name/");

        Long totalRow = s3Data.count();
        System.out.println(totalRow);

        //start:    19:01:25
        //end:      19:09:44
    }
}
