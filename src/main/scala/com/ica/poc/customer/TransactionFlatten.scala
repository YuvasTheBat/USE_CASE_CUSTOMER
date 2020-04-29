package com.ica.poc.customer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import com.ica.poc.session.SessionInit
import java.sql.Date
import java.text.SimpleDateFormat
import java.io.IOException
import com.ica.poc.session.SessionInit

/*
 * @author Yuvaraj Mani
 * @version 1.0
 * This object is used for flatten the customer transactions info data.
 * callCustomerTransaction will add sales_week_number,day_of_sales_week to the new dataframe for the further processes.
 */

object TransactionFlatten extends SessionInit{
  
  def getTransactionDataFrame(sprk:SparkSession) : DataFrame = {
    var transaction_flat: DataFrame = null
    try {
      transaction_flat = sprk.read.format("CSV").option("header", true)
        .csv("C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\transaction.csv")
    } catch {
      case e: IOException => {
        println("File Not Found For Transaction")
        println(e.printStackTrace())
      }
    }
    return transaction_flat
  }
  def callCustomerTransaction(transaction_flat:DataFrame) : DataFrame = {
    var transaction: DataFrame = null
    try {
      transaction = transaction_flat.withColumn("sales_week_number", date_format(col("date"), "w").cast(IntegerType))
        .withColumn("day_of_sales_week", date_format(col("date"), "E"))
    } catch {
      case e: Exception => {
        println("Exception in transaction dataframe")
        println(e.printStackTrace())
      }
    }
    transaction
  }
}