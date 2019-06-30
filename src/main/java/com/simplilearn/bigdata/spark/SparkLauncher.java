package com.simplilearn.bigdata.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class SparkLauncher {

    private static final Logger logger = LoggerFactory.getLogger(SparkLauncher.class);

    public static void main(String[] args) {

        SparkSession sparkSession = getSparkSession("spark-retail-transaction-analysis", "local[*]");
        Dataset<Row> dataset = readFile("/Users/raghugup/Downloads/dataset/all_mobile_phones_transaction.csv",readWithHeader(sparkSession));
//        topFiveBrandsSoldMost(dataset);
//        topFiveProductsSoldMost(dataset);
//        productCountAndRevenueQuarterly(dataset);
//        productCountAndRevenueQuarterlyByCategory(dataset);
//        productCountAndRevenueQuarterlyByBrand(dataset);
//        topFiveProfitableProductsSold(dataset);
//        topFiveProfitableBrandCategorywise(dataset);
//        bottomFiveeBrandByOrderCountAndCategorywise(dataset);
//        topFiveProfitableProductsSoldMonthly(dataset);
        bottomFiveLeastSellingProductsSoldMonthly(dataset);
    }

    private static SparkSession getSparkSession(String appName, String master) {
        SparkSession sparkSession = SparkSession.builder()
                .appName(appName)
                .master(master)
                .getOrCreate();
        print(new String[] {"Spark version"}, new Object[] {sparkSession.version()});
        UDFUtils.registerUDFs(sparkSession);
        return sparkSession;
    }

    private static SparkContext getSparkContezxt(String appName, String master) {
        return getSparkSession(appName, master).sparkContext();
    }

    private static DataFrameReader readWithHeader(SparkSession sparkSession) {
        return sparkSession.read()
                .option("header",true)
                .option("inferSchema", true)
                .option("mode", "DROPMALFORMED");
    }

    private static void print(String[] key, Object[] values) {
        StringBuilder logBuilder = new StringBuilder();
        for(int i =0 ;i < key.length ; i++) {
            logBuilder.append(key[i] +" = "+values[i])
                    .append("\n");
        }
        System.out.println(logBuilder.toString());
    }

    private static void print(String key, Object[] values, Object[] colNames) {
        StringBuilder logBuilder = new StringBuilder();
        logBuilder.append(key +" = \n");
        for(int i =0 ;i < colNames.length ; i++) {
            logBuilder.append(colNames[i])
                    .append(", ");
        }
        logBuilder.append("\n");
        for(int i =0 ;i < values.length ; i++) {
            logBuilder.append(values[i])
                    .append("\n");
        }
        System.out.println(logBuilder.toString());
    }


    private static Dataset<Row> readFile(String path, DataFrameReader dataFrameReader) {
        Dataset<Row> dataset = dataFrameReader.csv(path);
        print(new String[] {"Dataset Schema"}, new Object[] {dataset.schema()});
        print(new String[] {"Row Count"}, new Object[] {dataset.count()});
        return dataset;
    }

    /**
     * Solution
     *  5.a.i.1
     * @param dataset
     */
    private static void topFiveBrandsSoldMost(Dataset<Row> dataset) {
        dataset = dataset.withColumn("Month", functions.callUDF("toMonth", dataset.col("Invoice Date")));
        print(new String[] {"Dataset Schema"}, new Object[] {dataset.schema()});

        dataset = dataset.filter("SubCategory='mobile'").filter("month > 9 OR month = 0");
        print(new String[] {"Row Count"}, new Object[] {dataset.count()});
        dataset = dataset.select("month", "brand", "Quantity").groupBy("month", "brand").agg(functions.sum("Quantity").as("OrderCount"));

        List<Integer> months = Arrays.asList(0,10, 11);
        for(int i=0 ;i < months.size(); i++) {
            Dataset<Row> topSoldMobilePhones = dataset.filter("month = "+months.get(i)).sort(functions.desc("OrderCount")).limit(5)
                    .withColumn("Month Name", functions.callUDF("toMonthName", dataset.col("Month"))).drop("Month");
            print("Top 5 Brands having maximum Order in mobile category ",topSoldMobilePhones.collectAsList().toArray(), dataset.columns());
        }
    }

    /**
     * Solution
     *  5.a.i.2
     * @param dataset
     */
    private static void topFiveProductsSoldMost(Dataset<Row> dataset) {
        dataset = dataset.withColumn("Month", functions.callUDF("toMonth", dataset.col("Invoice Date")));
        print(new String[] {"Dataset Schema"}, new Object[] {dataset.schema()});

        dataset = dataset.filter("SubCategory='mobile'").filter("month > 9 OR month = 0");
        print(new String[] {"Row Count"}, new Object[] {dataset.count()});
        dataset = dataset.select("month", "Product Code", "Description", "Quantity").groupBy("month", "Product Code", "Description").agg(functions.sum("Quantity").as("OrderCount"));

        List<Integer> months = Arrays.asList(0,10, 11);
        for(int i=0 ;i < months.size(); i++) {
            Dataset<Row> topSoldMobileProducts = dataset.filter("month = "+months.get(i)).sort(functions.desc("OrderCount")).limit(5)
                    .withColumn("Month Name", functions.callUDF("toMonthName", dataset.col("Month"))).drop("Month");
            print("Top 5 products having maximum Order in mobile category ",topSoldMobileProducts.collectAsList().toArray(), dataset.columns());
        }
    }

    /**
     * Solution
     *  5.a.ii.1
     * @param dataset
     */
    private static void productCountAndRevenueQuarterly(Dataset<Row> dataset) {
        dataset = dataset.withColumn("Quarter", functions.callUDF("toQuarter", dataset.col("Invoice Date")));
        print(new String[] {"Dataset Schema"}, new Object[] {dataset.schema()});
        dataset = dataset.select("Quarter", "Transaction Amount", "Quantity").groupBy("Quarter").agg(functions.sum("Quantity").as("Inventory Sold"),
                functions.sum("Transaction Amount").as("Total Revenue"));
        Dataset<Row> topSoldMobileProducts = dataset
                .withColumn("Quarter Name", functions.callUDF("toQuarterName", dataset.col("Quarter")))
                .withColumn("Total Revenue", functions.callUDF("doubleToString", dataset.col("Total Revenue")))
                .drop("Quarter");
        print("Quarterly revenue and sold inventory ",topSoldMobileProducts.collectAsList().toArray(), dataset.columns());
    }

    /**
     * Solution
     *  5.a.ii.2
     * @param dataset
     */
    private static void productCountQuarterlyByCategory(Dataset<Row> dataset) {
        dataset = dataset.withColumn("Quarter", functions.callUDF("toQuarter", dataset.col("Invoice Date")));
        print(new String[] {"Dataset Schema"}, new Object[] {dataset.schema()});
        dataset = dataset.select("Quarter",  "Category", "Quantity").groupBy("Quarter", "Category").agg(functions.sum("Quantity").as("Inventory Sold"));
        Dataset<Row> topSoldMobileProducts = dataset
                .withColumn("Quarter Name", functions.callUDF("toQuarterName", dataset.col("Quarter")))
                .drop("Quarter");
        print("Quarterly sold inventory for each Category",topSoldMobileProducts.collectAsList().toArray(), dataset.columns());
    }

    /**
     * Solution
     *  5.a.ii.3
     * @param dataset
     */
    private static void productCountQuarterlyByBrand(Dataset<Row> dataset) {
        dataset = dataset.withColumn("Quarter", functions.callUDF("toQuarter", dataset.col("Invoice Date")));
        print(new String[] {"Dataset Schema"}, new Object[] {dataset.schema()});
        dataset = dataset.select("Quarter",  "Brand", "Quantity").groupBy("Quarter", "Brand").agg(functions.sum("Quantity").as("Inventory Sold"));
        Dataset<Row> topSoldMobileProducts = dataset
                .withColumn("Quarter Name", functions.callUDF("toQuarterName", dataset.col("Quarter")))
                .drop("Quarter");
        print("Quarterly sold inventory for each Brand",topSoldMobileProducts.collectAsList().toArray(), dataset.columns());
    }

    /**
     * Solution
     *  5.a.iii.1
     * @param dataset
     */
    private static void topFiveProfitableProductsSold(Dataset<Row> dataset) {
        dataset = dataset.select("Product Code", "Description", "Margin percentage", "Transaction Amount")
                            .withColumn("Profit", functions.callUDF("calculateProfit", dataset.col("Margin percentage"), dataset.col("Transaction Amount")))
                            .drop("Margin percentage", "Transaction Amount")
                            .groupBy("Product Code", "Description")
                            .agg(functions.sum("Profit").as("Profit"))
                            .sort(functions.desc("Profit"))
                            .limit(5);
        dataset = dataset.withColumn("Profit", functions.callUDF("doubleToString", dataset.col("Profit")));
        print("Top 5 products profitable products ",dataset.collectAsList().toArray(), dataset.columns());
    }

    /**
     * Solution
     *  5.a.iv.1
     * @param dataset
     */
    private static void topFiveProfitableBrandCategorywise(Dataset<Row> dataset) {
        dataset = dataset.select("Category", "SubCategory", "Brand", "Margin percentage", "Transaction Amount")
                .withColumn("Profit", functions.callUDF("calculateProfit", dataset.col("Margin percentage"), dataset.col("Transaction Amount")))
                .drop("Margin percentage", "Transaction Amount")
                .groupBy("Category", "SubCategory", "Brand")
                .agg(functions.sum("Profit").as("Profit"))
                .sort(functions.desc("Profit"))
                .limit(5);

        dataset = dataset.withColumn("Profit", functions.callUDF("doubleToString", dataset.col("Profit")));
        print("Top 5 profitable brands category wise ",dataset.collectAsList().toArray(), dataset.columns());
    }

    /**
     * Solution
     *  5.a.iv.2
     * @param dataset
     */
    private static void bottomFiveeBrandByOrderCountAndCategorywise(Dataset<Row> dataset) {
        dataset = dataset.select("Category", "SubCategory", "Brand", "Quantity")
                .groupBy("Category", "SubCategory", "Brand")
                .agg(functions.sum("Quantity").as("Quantity"))
                .sort(functions.asc("Quantity")).limit(5);
        print("Top 5 brands category wise which has less orders ",dataset.collectAsList().toArray(), dataset.columns());
    }

    /**
     * Solution
     *  5.a.v.1
     * @param dataset
     */
    private static void topFiveProductByOrderCount(Dataset<Row> dataset) {
        dataset = dataset.select("Product Code", "Description", "Quantity")
                .groupBy("Product Code", "Description")
                .agg(functions.sum("Quantity").as("Quantity"))
                .sort(functions.desc("Quantity")).limit(5);
        print("Top 5 product which has maximum orders = ",dataset.collectAsList().toArray(), dataset.columns());
    }

    /**
     * Solution
     *  5.a.v.2
     * @param dataset
     */
    private static void topFiveProfitableProductsSoldMonthly(Dataset<Row> dataset) {
        dataset = dataset.withColumn("Month", functions.callUDF("toMonth", dataset.col("Invoice Date")));
        dataset = dataset.select("Month", "Product Code", "Description", "Margin percentage", "Transaction Amount")
                .withColumn("Profit", functions.callUDF("calculateProfit", dataset.col("Margin percentage"), dataset.col("Transaction Amount")))
                .drop("Margin percentage", "Transaction Amount")
                .groupBy("Month", "Product Code", "Description")
                .agg(functions.sum("Profit").as("Profit"));
        for(int i=0 ;i < 12; i++) {
            Dataset<Row> topSoldMobilePhones = dataset.filter("month = "+i).sort(functions.desc("Profit"))
                    .withColumn("Profit", functions.callUDF("doubleToString", dataset.col("Profit"))).limit(5)
                    .withColumn("Month Name", functions.callUDF("toMonthName", dataset.col("Month"))).drop("Month");
            print("Top 5 products profitable products monthly ",topSoldMobilePhones.collectAsList().toArray(), dataset.columns());
        }
    }

    /**
     * Solution
     *  5.a.v.3
     * @param dataset
     */
    private static void bottomFiveLeastSellingProductsSoldMonthly(Dataset<Row> dataset) {
        dataset = dataset.withColumn("Month", functions.callUDF("toMonth", dataset.col("Invoice Date")));
        dataset = dataset.select("Month", "Product Code", "Description", "Quantity")
                .groupBy("Month", "Product Code", "Description")
                .agg(functions.sum("Quantity").as("Quantity"));
        for(int i=0 ;i < 12; i++) {
            Dataset<Row> topSoldMobilePhones = dataset.filter("month = "+i).sort(functions.desc("Quantity")).limit(5)
                    .withColumn("Month Name", functions.callUDF("toMonthName", dataset.col("Month"))).drop("Month");
            print("Botton 5 products least selling products monthly ",topSoldMobilePhones.collectAsList().toArray(), dataset.columns());
        }
    }
}
