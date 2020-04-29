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
 * This object is used for flatten the customer info data.
 * callCustomerInfo will add age,age_group to the new dataframe for the further processes.
 */

object CustomerInfoFlatten extends SessionInit {
  
  def getCustomerInfoDataFrame(sprk:SparkSession) : DataFrame = {
   var customer_flat:DataFrame = null;
   try {
    customer_flat = sprk.read.format("CSV").option("header", true)
        .csv("C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\dob.csv")
   }
   catch {
     case e:IOException => {
       println("FileNotFound Exception")
       println(e.printStackTrace())
     }
   }
   return customer_flat
 }
  
 def callCustomerInfo(customer_flat:DataFrame) : DataFrame = {
   var result_customer:DataFrame = null;
    try {
      val time_s = System.currentTimeMillis()
      val currentDate = new Date(time_s)
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

      val getAge = udf((dob: String) => {
        val dateUtil = dateFormat.parse(dob)
        val dob_date = new Date(dateUtil.getTime)
        currentDate.getYear - dob_date.getYear
      })
      import sprk.implicits._
      val cust_info = customer_flat.withColumn("age", getAge(col("cust_dob")))
      result_customer = cust_info.withColumn("age_group", when(col("age").>=(15) && col("age").<=(20), "20_age_group")
        .when(col("age").>(21) && col("age").<=(25), "25_age_group")
        .when(col("age").>(25) && col("age").<=(31), "30_age_group")
        .otherwise("unknown"))
    } 
    catch {
      case e: Exception =>
        {
          println(e.printStackTrace())
        }
    } 
    return result_customer
  }
  
}