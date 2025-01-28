package org.ergemp.udfExamples;

import org.apache.spark.sql.*;
import org.ergemp.udfExamples.model.Employee;

public class AggregateExample {

    // ref: https://spark.apache.org/docs/latest/sql-ref-functions-udf-aggregate.html

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("org.ergemp.udfExamples.AggregateExample")
                .master("local")
                .getOrCreate();

        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        String path = "data/employees.json";
        Dataset<Employee> df = spark.read().json(path).as(employeeEncoder);
        df.createOrReplaceTempView("employees");
        df.show();

        // typed example
        MyAverageTyped myAverage = new MyAverageTyped();
        // Convert the function to a `TypedColumn` and give it a name
        TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
        Dataset<Double> result = df.select(averageSalary);
        result.show();

        // untyped example
        // Register the function to access it
        spark.udf().register("myAverage", functions.udaf(new MyAverageUntyped(), Encoders.LONG()));
        Dataset<Row> result2 = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
        result2.show();
    }
}
