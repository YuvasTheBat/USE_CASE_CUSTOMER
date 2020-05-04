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
import scala.util.{Try,Success,Failure}
import java.io.FileNotFoundException

/*
 * @author Yuvaraj Mani
 * @version 1.0
 * This object is used for flatten the customer offer info data.
 * customerOfferCall will add begin_week_number,end_week_number to the new dataframe for the further processes.
 */

object CustomerOfferFlatten extends SessionInit {
  
  def getCustomerOfferDataFrame(sprk:SparkSession) : DataFrame = {
   val customerOffer =  Try ({
      val customerOfferFlat = sprk.read.format("CSV")
        .option("header", true).option("mode", "DROPMALFORMED")
        .csv("C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\offer.csv")
       customerOfferFlat 
    })
     customerOffer match {
      case Success(v) => v
      case Failure(issue) =>
        throw new FileNotFoundException("FileNotFound For Customer Offers")
    }
  }
  
  def getWeeksOfYear(sprk:SparkSession) : DataFrame = {
    val numberTab = Try ({
     val numberTabForYear = sprk.read.format("CSV")
        .option("header", true)
        .csv("C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\number.csv").select(col("number").alias("week_number").cast(IntegerType))
        numberTabForYear
    })
    numberTab match {
      case Success(v) => v
      case Failure(issue) =>
        throw new FileNotFoundException("FileNotFound For Numbers Table")
    }
  }
  
  def customerOfferCall(offerFlat:DataFrame,numberTab:DataFrame) : DataFrame = {
      val offer = offerFlat.withColumn("begin_week_number", date_format(col("begin_date"), "w").cast(IntegerType))
        .withColumn("end_week_number", date_format(col("end_date"), "w").cast(IntegerType))

     val customerOfferCall = offer.join(numberTab, numberTab("week_number")
        .between(offer("begin_week_number"), offer("end_week_number")))
        .select("week_number", "offer_id", "offer_name", "cust_id", "begin_date", "end_date", "begin_week_number", "end_week_number")
    customerOfferCall
  }
}