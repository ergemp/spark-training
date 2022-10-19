package org.ergemp.rdd.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;

import java.sql.DriverManager;

public class CreateRDDFromJdbc2 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("CreateRDDFromJdbc2").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Object[]> rdd = JdbcRDD.create(
                jsc,
                () -> DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres"),
                //"SELECT json_content FROM mytable WHERE ? <= ID AND ID <= ?",
                "SELECT json_content FROM mytable where ? <= ?",
                1, 100, 1
        ).cache();

        rdd.foreach(each -> System.out.println(each[0].toString()));
    }
}

/*
* https://spark.apache.org/docs/2.4.7/api/java/org/apache/spark/rdd/JdbcRDD.html
* public static JavaRDD<Object[]> create(JavaSparkContext sc,
                                       JdbcRDD.ConnectionFactory connectionFactory,
                                       String sql,
                                       long lowerBound,
                                       long upperBound,
                                       int numPartitions)
Create an RDD that executes a SQL query on a JDBC connection and reads results. Each row is converted into a Object array. For usage example, see test case JavaAPISuite.testJavaJdbcRDD.

Parameters:
connectionFactory - a factory that returns an open Connection. The RDD takes care of closing the connection.
sql - the text of the query. The query must contain two ? placeholders for parameters used to partition the results. For example,

   select title, author from books where ? <= id and id <= ?

lowerBound - the minimum value of the first placeholder
upperBound - the maximum value of the second placeholder The lower and upper bounds are inclusive.
numPartitions - the number of partitions. Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2, the query would be executed twice, once with (1, 10) and once with (11, 20)
*/
