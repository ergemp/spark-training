package org.ergemp.rdd.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.ImmutableList;

import java.io.Serializable;
import java.util.List;

public class CreateRDDFromImmutableList {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("CreateRDDFromImmutableList").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // prepare list of objects
        List<Person> personList = ImmutableList.of(
                new Person("Arjun", 25),
                new Person("Akhil", 2));

        // parallelize the list using SparkContext
        JavaRDD<Person> perJavaRDD = jsc.parallelize(personList);

        for(Person person : perJavaRDD.collect()){
            System.out.println(person.name);
        }

        jsc.close();
    }

    static class Person implements Serializable {
        private static final long serialVersionUID = -2685444218382696366L;
        String name;
        int age;
        public Person(String name, int age){
            this.name = name;
            this.age = age;
        }
    }
}
