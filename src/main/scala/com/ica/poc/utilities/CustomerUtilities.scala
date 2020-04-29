package com.ica.poc.utilities

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import com.ica.poc.session.SessionInit
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.DataFrame

/*
 * @author Yuvaraj Mani
 * @version 1.0
 * This object contains methods of each functionality which will be called from CustomerStatus main object..
 * callCustomerTransaction will add sales_week_number,day_of_sales_week to the new dataframe for the further processes.
 */

object CustomerUtilities extends SessionInit{
  
  def customerWiseweeklyStatus(customer_call: DataFrame, customer_offer_call: DataFrame, transaction_res: DataFrame): DataFrame = {

    val res_trans = transaction_res.select(col("sales_week_number"), col("sales").cast(DoubleType), col("customer_id"), col("offer_id").alias("offer_redeemed"))
      .join(
        customer_offer_call.select(col("week_number"), col("offer_id").alias("offer_received"), col("cust_id")),
        transaction_res("sales_week_number") === customer_offer_call("week_number") && transaction_res("customer_id") === customer_offer_call("cust_id"), "left")
      .select("sales_week_number", "sales", "customer_id", "offer_received", "offer_redeemed")

    val res_trans_1 = res_trans.join(customer_call.select(col("age"), col("age_group"), col("cust_id")), res_trans("customer_id") === customer_call("cust_id"), "left")
      .select("sales_week_number", "sales", "customer_id", "offer_received", "offer_redeemed", "age", "age_group")

    val res_each_cust_data = res_trans_1.groupBy("sales_week_number", "customer_id", "age", "age_group")
      .agg(sum("sales").as("sales_sum"), count("offer_received").as("offer_received_count"),
        count("offer_redeemed").as("offer_redeemed_count"), count("sales_week_number").as("no_of_visit"))
      .orderBy(col("sales_week_number").asc, col("customer_id").cast(IntegerType).asc)

    return res_each_cust_data

  }

  def customerOverAllWeeklyStatus(customerWiseweeklyStatus: DataFrame): DataFrame = {

    val overall_count = customerWiseweeklyStatus.groupBy("sales_week_number").agg(
      count("customer_id").cast(IntegerType).as("customer_count"),
      sum("offer_received_count").as("total_offer_received"), sum("offer_redeemed_count").as("total_offer_redeemed"), sum("sales_sum").as("total_amount"), sum("no_of_visit").as("no_of_visit"))
      .orderBy(col("sales_week_number").cast(IntegerType).asc) //.show()

    return overall_count

  }

  def transaction_res(transaction: DataFrame, customer_call: DataFrame): DataFrame = {

    val res_transaction = transaction.join(customer_call, transaction("customer_id") === customer_call("cust_id"), "inner")
      .select("transaction_id", "customer_id", "product_id", "store_id", "offer_id", "sales", "date", "sales_week_number", "day_of_sales_week", "age_group")

    return res_transaction
  }

  def transactionOnAgeGroupWise(res_transaction: DataFrame): DataFrame = {

    val res_age_group = res_transaction.groupBy("day_of_sales_week", "age_group").agg(count("day_of_sales_week").as("days_count"))
    val res_age_group_1 = res_age_group.groupBy("age_group").agg(max("days_count").as("total"))
    val result = res_age_group.join(res_age_group_1, res_age_group("age_group") === res_age_group_1("age_group") && res_age_group("days_count") === res_age_group_1("total"))
      .select(res_age_group("day_of_sales_week"), res_age_group("age_group"), res_age_group_1("total"))

    return result
  }


  
}