package com.ica.poc.customer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import com.ica.poc.session.SessionInit
import java.io.IOException
import com.ica.poc.session.SessionInit

/*
 * @author Yuvaraj Mani
 * @version 1.0
 * This object is used for flatten the customer offer info data.
 * customerOfferCall will add begin_week_number,end_week_number to the new dataframe for the further processes.
 */

object CustomerOfferFlatten extends SessionInit {
  
  def getCustomerOfferDataFrame(sprk:SparkSession) : DataFrame = {
    var offer_flat: DataFrame = null
    try {
      offer_flat = sprk.read.format("CSV")
        .option("header", true)
        .csv("C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\offer.csv")
    } catch {
      case e: IOException => {
        println("File Not Found")
        println(e.printStackTrace())
      }
    }
    return offer_flat
  }
  
  def getWeeksOfYear(sprk:SparkSession) : DataFrame = {
    var number_tab: DataFrame = null
    try {
      number_tab = sprk.read.format("CSV")
        .option("header", true)
        .csv("C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\number.csv").select(col("number").alias("week_number").cast(IntegerType))
    } catch {
      case e: IOException => {
        println("File Not Found")
        println(e.printStackTrace())
      }
    }
    return number_tab
  }
  
  def customerOfferCall(offer_flat:DataFrame,number_tab:DataFrame) : DataFrame = {
    var offer_join_res: DataFrame = null
    try {
      val offer = offer_flat.withColumn("begin_week_number", date_format(col("begin_date"), "w").cast(IntegerType))
        .withColumn("end_week_number", date_format(col("end_date"), "w").cast(IntegerType))

      offer_join_res = offer.join(number_tab, number_tab("week_number")
        .between(offer("begin_week_number"), offer("end_week_number")))
        .select("week_number", "offer_id", "offer_name", "cust_id", "begin_date", "end_date", "begin_week_number", "end_week_number")
    } catch {
      case e: Exception => {
        println("Exception in offer flatten dataframe")
        println(e.printStackTrace())
      }
    }
    return offer_join_res
  }
}