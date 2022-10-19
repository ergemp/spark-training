package org.ergemp.sql.functions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class AggregateFunctions {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("AggregateFunctions")
                .getOrCreate();

        //StructType schema = new StructType().add("customer_id","Integer");
        //Dataset<Row> df = spark.read().schema(schema).json("data/customerSales/spark_training_customers.json");

        Dataset<Row> df = spark.read().option("inferschema",true).json("data/customerSales/spark_training_sales.json");
        df.printSchema();

        // approx_count_distinct() function returns the count of distinct items in a group.
        System.out.println("approx_count_distinct: " + df.select(approx_count_distinct("price_pp")).collectAsList().get(0));
        //1026

        // collect_list() function returns all values from an input column with duplicates.
        df.select(collect_list("price_pp")).show();

        // collect_set() function returns all values from an input column with duplicate values eliminated.
        df.select(collect_set("price_pp")).show();

        // countDistinct() function returns the number of distinct elements in a columns
        df.select(countDistinct("customer_id")).show();
        //100

        // count() function returns number of elements in a column.
        System.out.println("count: "+df.select(count("product_id")).collectAsList().get(0));
        //10000

        // first() function returns the first element in a column
        // when ignoreNulls is set to true, it returns the first non-null element.
        df.select(first("product_id")).show();
        df.select(first("product_id", true)).show();

        // last() function returns the last element in a column.
        // when ignoreNulls is set to true, it returns the last non-null element.
        df.select(last("product_id")).show();
        df.select(last("product_id", true)).show();

        /*
        Kurtosis is a measure of whether the data are heavy-tailed or light-tailed relative to a normal distribution.
        That is, data sets with high kurtosis tend to have heavy tails, or outliers.
        Data sets with low kurtosis tend to have light tails, or lack of outliers.
        A uniform distribution would be the extreme case.
        */

        df.select(kurtosis("price_pp")).show();

        // max() function returns the maximum value in a column.
        df.select(max("price_pp")).show();

        // min() function
        df.select(min("price_pp")).show();

        // mean() function returns the average of the values in a column. Alias for Avg
        df.select(mean("price_pp")).show();

        /*
        skewness tells us about the direction of outliers.
        You can see that our distribution is positively skewed and
        most of the outliers are present on the right side of the distribution.
        Note: The skewness does not tell us about the number of outliers. It only tells us the direction
        */

        df.select(skewness("price_pp")).show();

        /*
        stddev() alias for stddev_samp.
        stddev_samp() function returns the sample standard deviation of values in a column.
        stddev_pop() function returns the population standard deviation of the values in a column.
        */

        df.select(stddev("price_pp"), stddev_samp("price_pp"), stddev_pop("price_pp")).show();

        // sum() function Returns the sum of all values in a column.
        df.select(sum("price_pp")).show();

        // sumDistinct() function returns the sum of all distinct values in a column.
        df.select(sumDistinct("price_pp")).show();

        /*
        variance() alias for var_samp
        var_samp() function returns the unbiased variance of the values in a column.
        var_pop() function returns the population variance of the values in a column.

        Subtract the mean from each data value and square the result.
        Find the sum of all the squared differences.
        The sum of squares is all the squared differences added together.
        Calculate the variance.

        The variance is a measure of variability.
        It is calculated by taking the average of squared deviations from the mean.
        Variance tells you the degree of spread in your data set.
        The more spread the data, the larger the variance is in relation to the mean.
        */
        df.select(variance("price_pp"), var_samp("price_pp"), var_pop("price_pp")).show();


    }
}
