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
import scala.util.{Try,Success,Failure}

/*
 * @author Yuvaraj Mani
 * @version 1.0
 * This object contains methods of each functionality which will be called from CustomerStatus main object..
 * callCustomerTransaction will add sales_week_number,day_of_sales_week to the new dataframe for the further processes.
 */

object CustomerUtilities extends SessionInit{
  
  def customerWiseweeklyStatus(customerCall: DataFrame, customerOfferCall: DataFrame, transactionRes: DataFrame): DataFrame = {
    
        val transactionFlatten = transactionRes.select(col("sales_week_number"), col("sales").cast(DoubleType), col("customer_id"), col("offer_id").alias("offer_redeemed"))
          .join(
            customerOfferCall.select(col("week_number"), col("offer_id").alias("offer_received"), col("cust_id")),
            transactionRes("sales_week_number") === customerOfferCall("week_number") && transactionRes("customer_id") === customerOfferCall("cust_id"), "left")
          .select("sales_week_number", "sales", "customer_id", "offer_received", "offer_redeemed")

        val transactionFlattenForWeeklyStatus = transactionFlatten.join(customerCall.select(col("age"), col("age_group"), col("cust_id")), transactionFlatten("customer_id") === customerCall("cust_id"), "left")
          .select("sales_week_number", "sales", "customer_id", "offer_received", "offer_redeemed", "age", "age_group")

        val resultOfWeeklyTransaction = transactionFlattenForWeeklyStatus.groupBy("sales_week_number", "customer_id", "age", "age_group")
          .agg(sum("sales").as("sales_sum"), count("offer_received").as("offer_received_count"),
            count("offer_redeemed").as("offer_redeemed_count"), count("sales_week_number").as("no_of_visit"))
          .orderBy(col("sales_week_number").asc, col("customer_id").cast(IntegerType).asc)
        resultOfWeeklyTransaction
  }

  def customerOverAllWeeklyStatus(customerWiseweeklyStatus: DataFrame): DataFrame = {
    Try {
    val customerOverAllWeeklyStatus = customerWiseweeklyStatus.groupBy("sales_week_number").agg(
      count("customer_id").cast(IntegerType).as("customer_count"),
      sum("offer_received_count").as("total_offer_received"), sum("offer_redeemed_count").as("total_offer_redeemed"), sum("sales_sum").as("total_amount"), sum("no_of_visit").as("no_of_visit"))
      .orderBy(col("sales_week_number").cast(IntegerType).asc) //.show()

     customerOverAllWeeklyStatus
    }
    match {
      case Success(v)=>v
      case Failure(issue) =>
        throw new NullPointerException("customerWiseweeklyStatus is null")
    }

  }

  def customerTransactionResult(transaction: DataFrame, customerCall: DataFrame): DataFrame = {

    val transactionResult = transaction.join(customerCall, transaction("customer_id") === customerCall("cust_id"), "inner")
      .select("transaction_id", "customer_id", "product_id", "store_id", "offer_id", "sales", "date", "sales_week_number", "day_of_sales_week", "age_group")

    return transactionResult
  }

  def transactionOnAgeGroupWise(transactionAgeGroup: DataFrame): DataFrame = {
    Try {
    val resultOfAgeGroup = transactionAgeGroup.groupBy("day_of_sales_week", "age_group").agg(count("day_of_sales_week").as("days_count"))
    val resultOfAgeGroupFetch = resultOfAgeGroup.groupBy("age_group").agg(max("days_count").as("total"))
    val resultAgeGroup = resultOfAgeGroup.join(resultOfAgeGroupFetch, resultOfAgeGroup("age_group") === resultOfAgeGroupFetch("age_group") && resultOfAgeGroup("days_count") === resultOfAgeGroupFetch("total"))
      .select(resultOfAgeGroup("day_of_sales_week"), resultOfAgeGroup("age_group"), resultOfAgeGroupFetch("total"))
    resultAgeGroup
    }
    match {
      case Success(v)=>v
      case Failure(issue) =>
        throw new NullPointerException("transactionAgeGroup is null")
    }
  }


  
}