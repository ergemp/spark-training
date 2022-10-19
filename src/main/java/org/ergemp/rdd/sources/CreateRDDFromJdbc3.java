package org.ergemp.rdd.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import scala.reflect.ClassManifestFactory$;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class CreateRDDFromJdbc3 {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("spark JdbcRDD example").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        DBConnection dbConnection = new DBConnection();
        JdbcRDD<Object[]> jdbcRDD = new JdbcRDD(sc.sc(),
                dbConnection,
                "select * from pg_user where ?=?",
                0L,
                0L ,
                1,
                new MapResult(),
                ClassManifestFactory$.MODULE$.fromClass(Object[].class));

        //JavaRDD<Object[]> javaRDD = JavaRDD.fromRDD(jdbcRDD, ClassManifestFactory$.MODULE$.fromClass(Object[].class));
        JavaRDD<Object[]> javaRDD = jdbcRDD.toJavaRDD();

        //javaRDD.map(record -> record[0].toString());
        List<String> lines = javaRDD.map(record -> record[0].toString()).collect();
        /*
        List<String> lines = javaRDD.map(new Function<Object[], String>() {
            @Override
            public String call(final Object[] record) throws Exception {
                return record[0].toString();
            }
        }).collect();
        */
        lines.forEach(line -> System.out.println(line));
    }

    public static class MapResult extends AbstractFunction1<ResultSet, Object[]> implements Serializable {
        public Object[] apply(ResultSet row) {
            return JdbcRDD.resultSetToObjectArray(row);
        }
    }

    public static class DBConnection extends AbstractFunction0<Connection> implements Serializable {

        private String driverClassName;
        private String connectionUrl;
        private String userName;
        private String password;

        public DBConnection(String driverClassName, String connectionUrl, String userName, String password) {
            this.driverClassName = driverClassName;
            this.connectionUrl = connectionUrl;
            this.userName = userName;
            this.password = password;
        }

        public DBConnection() {
            this.driverClassName = "org.postgresql.Driver";
            this.connectionUrl = "jdbc:postgresql://localhost/postgres";
            this.userName = "postgres";
            this.password = "";
        }

        @Override
        public Connection apply() {
            try {
                Class.forName(driverClassName);
            } catch (ClassNotFoundException e) {
                //LOGGER.error("Failed to load driver class", e);
                e.printStackTrace();
            }

            Properties properties = new Properties();
            properties.setProperty("user", userName);
            properties.setProperty("password", password);

            Connection connection = null;
            try {
                connection = DriverManager.getConnection(connectionUrl, properties);
            } catch (SQLException e) {
                //LOGGER.error("Connection failed", e);
                e.printStackTrace();
            }
            return connection;
        }
    }
}
