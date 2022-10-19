package org.ergemp.workshop.productSales.withRDD.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.ergemp.workshop.productSales.withRDD.function.CountData;
import org.ergemp.workshop.productSales.withRDD.function.JoinData;
import org.ergemp.workshop.productSales.withRDD.function.ModifyData;
import scala.Tuple2;

import java.util.Map;

public class run {
    public static void main(String[] args){

        String customers = "resources/customerSales/spark_training_customers.csv";
        String sales = "resources/customerSales/spark_training_sales.csv";

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkJoinsWithRDD").setMaster("local"));

        //sales_id,customer_id,product_id,date,ts,amount,price_pp
        JavaRDD<String> salesInputFile = sc.textFile(sales);

        JavaPairRDD<Integer, Integer> salesPairs =
                salesInputFile
                        .filter(each -> !each.contains("sales_id"))
                        .mapToPair(
                            new PairFunction<String, Integer, Integer>() {
                                public Tuple2<Integer, Integer> call(String s) {
                                    String[] transactionSplit = s.split(",");
                                    return new Tuple2<Integer, Integer>(Integer.valueOf(transactionSplit[1]), Integer.valueOf(transactionSplit[0]));
                                }
                            }
                        );

        //customer_id,first_name,last_name,email,gender,ip_address,city,country
        JavaRDD<String> customerInputFile = sc.textFile(customers);

        JavaPairRDD<Integer, String> customerPairs =
                customerInputFile
                        .filter(each -> !each.contains("customer_id"))
                        .mapToPair(
                            new PairFunction<String, Integer, String>() {
                                public Tuple2<Integer, String> call(String s) {
                                    String[] customerSplit = s.split(",");
                                    return new Tuple2<Integer, String>(Integer.valueOf(customerSplit[0]), customerSplit[6]);
                                }
                            }
                        );

        Map<String, Long> result = CountData.count(ModifyData.modify(JoinData.join(salesPairs, customerPairs)));
        result.forEach((k, v) -> System.out.println((k + ":" + v)));

        //output_rdd.saveAsHadoopFile(args[2], String.class, String.class, TextOutputFormat.class);
        sc.close();
    }
}

