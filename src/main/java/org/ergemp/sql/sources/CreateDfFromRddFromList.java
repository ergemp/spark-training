package org.ergemp.sql.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CreateDfFromRddFromList {
    public static void main(String[] args) {

        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromRddFromList")
                .master("local")
                .getOrCreate();

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        // Generate Data
        List<StringWrapper> nums = new ArrayList<>();
        nums.add(new StringWrapper("value1", "value2"));

        // Convert to RDD
        JavaRDD<StringWrapper> rdd = jsc.parallelize(nums);

        spark.createDataFrame(rdd, StringWrapper.class).show(false);

    }

    public static class StringWrapper implements Serializable {
        private String key;
        private String value;

        public StringWrapper(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
