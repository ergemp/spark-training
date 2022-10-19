package org.ergemp.rdd.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;

import java.sql.DriverManager;

public class CreateRDDFromJdbc {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("CreateRDDFromJdbc").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = JdbcRDD.create(
                jsc,
                () -> DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres"),
                //"SELECT json_content FROM mytable WHERE ? <= ID AND ID <= ?",
                "SELECT json_content FROM mytable where ? <= ?",
                1, 100, 1,
                r -> r.getString(1)
        ).cache();

        rdd.foreach(each -> System.out.println(each));
    }
}

/*
*
* https://spark.apache.org/docs/2.4.7/api/java/org/apache/spark/rdd/JdbcRDD.html
* An RDD that executes a SQL query on a JDBC connection and reads results. For usage example, see test case JdbcRDDSuite.
param: getConnection a function that returns an open Connection. The RDD takes care of closing the connection.
param: sql the text of the query. The query must contain two ? placeholders for parameters used to partition the results. For example,

select title, author from books where ? <= id and id <= ?

param: lowerBound the minimum value of the first placeholder param: upperBound the maximum value of the second placeholder The lower and upper bounds are inclusive.
param: numPartitions the number of partitions. Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2, the query would be executed twice, once with (1, 10) and once with (11, 20)
param: mapRow a function from a ResultSet to a single row of the desired result type(s). This should only call getInt, getString, etc; the RDD takes care of calling next. The default maps a ResultSet to an array of Object.
*
*/
