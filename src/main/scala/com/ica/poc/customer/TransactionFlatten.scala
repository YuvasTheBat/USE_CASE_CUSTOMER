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
import scala.util.{Try,Success,Failure}
import java.io.FileNotFoundException

/*
 * @author Yuvaraj Mani
 * @version 1.0
 * This object is used for flatten the customer transactions info data.
 * callCustomerTransaction will add sales_week_number,day_of_sales_week to the new dataframe for the further processes.
 */

object TransactionFlatten extends SessionInit{
  
  def getTransactionDataFrame(sprk:SparkSession) : DataFrame = {
   val transaction = Try ({
     val  transactionFlat = sprk.read.format("CSV").option("header", true).option("mode", "DROPMALFORMED")
        .csv("C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\transaction.csv")
        transactionFlat
    })
    transaction match {
     case Success(v) => v
     case Failure(issue) =>
       throw new FileNotFoundException("FileNotFound for Customer Transaction")
     
   }
  }
  def callCustomerTransaction(transactionFlat:DataFrame) : DataFrame = {
      val transaction = transactionFlat.withColumn("sales_week_number", date_format(col("date"), "w").cast(IntegerType))
        .withColumn("day_of_sales_week", date_format(col("date"), "E"))
    transaction
  }
}