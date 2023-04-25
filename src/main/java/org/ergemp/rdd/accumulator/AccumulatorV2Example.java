package org.ergemp.rdd.accumulator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;


import java.io.Serializable;
import java.util.Vector;

public class AccumulatorV2Example {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("SimpleAccumulatorExample").setMaster("local[*]");
        SparkContext sparkContext = new SparkContext(conf);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

        JavaRDD<String> file1 = javaSparkContext.textFile("data/nasa-weblogs.txt");

        VectorAccumulatorV2 myVectorAcc = new VectorAccumulatorV2();
        javaSparkContext.sc().register(myVectorAcc, "MyVectorAcc1");

        file1.foreach(line -> {
            myVectorAcc.add(line);
        });

        System.out.println(myVectorAcc.toString());
    }

    public static class VectorAccumulatorV2 extends AccumulatorV2<String, Vector> implements Serializable  {

        private Vector<String> myVector = new Vector<String>();

        public VectorAccumulatorV2 () {

        }

        // Returns if this accumulator is zero value or not.
        @Override
        public boolean isZero() {
            if (myVector.size() > 0) {
                return false;
            }
            else {
                return true;
            }
        }

        // Creates a new copy of this accumulator.
        @Override
        public AccumulatorV2<String, Vector> copy() {
            return this;
        }

        // Resets this accumulator, which is zero value. i.e. call isZero must return true.
        @Override
        public void reset() {
            myVector.clear();
        }

        //Takes the inputs and accumulates.
        @Override
        public void add(String s) {
            myVector.add(s);

        }

        //Merges another same-type accumulator into this one and update its state, i.e. this should be merge-in-place.
        @Override
        public void merge(AccumulatorV2<String, Vector> other) {
            this.myVector.add(other.value().toString());
        }

        //Defines the current value of this accumulator
        @Override
        public Vector value() {
            return this.myVector;
        }

        //copyAndReset
        //Creates a new copy of this accumulator, which is zero value. i.e. call isZero on the copy must return true.
        //assertion failed: copyAndReset must return a zero value copy

        //reset
        //Resets this accumulator, which is zero value.
    }
}

// reference: https://spark.apache.org/docs/latest/api/scala/org/apache/spark/util/AccumulatorV2.html