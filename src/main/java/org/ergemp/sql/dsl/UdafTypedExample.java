package org.ergemp.sql.dsl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

public class UdafTypedExample {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("UdafTypedExample")
                .getOrCreate();

        // User-Defined Aggregate Functions (UDAFs) are user-programmable routines that act on multiple rows at once
        // and return a single aggregated value as a result.

        // Type-Safe User-Defined Aggregate Functions
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        String path = "data/spark/employees.json";

        Dataset<Employee> ds = spark.read().json(path).as(employeeEncoder);
        ds.show();

        MyAverage myAverage = new MyAverage();
        // Convert the function to a `TypedColumn` and give it a name
        TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
        Dataset<Double> result = ds.select(averageSalary);
        result.show();

    }

    public static class Employee implements Serializable {
        private String name;
        private long salary;

        // Constructors, getters, setters...

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getSalary() {
            return salary;
        }

        public void setSalary(long salary) {
            this.salary = salary;
        }
    }

    public static class Average implements Serializable  {
        private long sum;
        private long count;

        // Constructors, getters, setters...

        public Average(){
            this.count=0L;
            this.sum=0L;
        }
        public Average(Long gSum, Long gCount) {
            this.count=gCount;
            this.sum=gSum;
        }

        public long getSum() {
            return sum;
        }

        public void setSum(long sum) {
            this.sum = sum;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }

    public static class MyAverage extends Aggregator<Employee, Average, Double> {

        // A zero value for this aggregation. Should satisfy the property that any b + zero = b
        public Average zero() {
            return new Average(0L, 0L);
        }

        // Combine two values to produce a new value. For performance, the function may modify `buffer`
        // and return it instead of constructing a new object
        public Average reduce(Average buffer, Employee employee) {
            long newSum = buffer.getSum() + employee.getSalary();
            long newCount = buffer.getCount() + 1;
            buffer.setSum(newSum);
            buffer.setCount(newCount);
            return buffer;
        }

        // Merge two intermediate values
        public Average merge(Average b1, Average b2) {
            long mergedSum = b1.getSum() + b2.getSum();
            long mergedCount = b1.getCount() + b2.getCount();
            b1.setSum(mergedSum);
            b1.setCount(mergedCount);
            return b1;
        }

        // Transform the output of the reduction
        public Double finish(Average reduction) {
            return ((double) reduction.getSum()) / reduction.getCount();
        }

        // Specifies the Encoder for the intermediate value type
        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }

        // Specifies the Encoder for the final output value type
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }

    }

}

// ref: https://spark.apache.org/docs/latest/sql-ref-functions-udf-aggregate.html