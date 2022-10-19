package org.ergemp.rdd.streaming.transformations;

public class Transform {
    public static void main(String[] args) {
        /*
        * From spark documentation:
        * The transform operation allows arbitrary RDD-to-RDD functions to be applied on a DStream.
        * It can be used to apply any RDD operation that is not exposed in the DStream API.
        *
        * For example, the functionality of joining every batch in a data stream
        * with another dataset is not directly exposed in the DStream API.
        *
        * However, you can easily use transform to do this.
        * This enables very powerful possibilities.
        *
        * For example, one can do real-time data cleaning by joining the input data stream with precomputed
        * spam information (maybe generated with Spark as well) and then filtering based on it.
        *
        * Note that the supplied function gets called in every batch interval.
        * This allows you to do time-varying RDD operations, that is, RDD operations,
        * number of partitions, broadcast variables, etc. can be changed between batches.
        * */

        /*
        import org.apache.spark.streaming.api.java.*;
        // RDD containing spam information
        JavaPairRDD<String, Double> spamInfoRDD = jssc.sparkContext().newAPIHadoopRDD(...);

        JavaPairDStream<String, Integer> cleanedDStream = wordCounts.transform(rdd -> {
          rdd.join(spamInfoRDD).filter(...); // join data stream with spam information to do data cleaning
          ...
        });
        */


    }
}
