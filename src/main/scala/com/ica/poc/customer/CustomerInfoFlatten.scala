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
import java.text.ParseException
import java.io.IOException
import com.ica.poc.session.SessionInit
import scala.util.{Try,Success,Failure}
import java.io.FileNotFoundException


/*
 * @author Yuvaraj Mani
 * @version 1.0
 * This object is used for flatten the customer info data.
 * callCustomerInfo will add age,age_group to the new dataframe for the further processes.
 */

object CustomerInfoFlatten extends SessionInit {
  
  def getCustomerInfoDataFrame(sprk:SparkSession) : DataFrame = {
    val customerInfo = Try({
      val customerFlat = sprk.read.format("CSV").option("header", true).option("mode", "DROPMALFORMED")
        .csv("C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\dob.csv")
        customerFlat
    })
    customerInfo match {
      case Success(v) => v
      case Failure(issue) =>
        throw new FileNotFoundException("FileNotFound for customer's info")
    }
  }
  
   def getCustomerInfoDataFrame123(sprk:SparkSession) : DataFrame = {
    val customerInfo = Try({
      val customerFlat = sprk.read.format("CSV").option("header", true).option("mode", "DROPMALFORMED")
        .csv("C:\\Users\\yuvas\\Azure_Study\\VISA\\EASy\\SOURCE\\input\\dob.csv")
      customerFlat
    })
    customerInfo match {
      case Success(v) => v
      case Failure(issue) =>
        throw new FileNotFoundException("FileNotFound for customer's info")
    }
  }
    val timesec = System.currentTimeMillis()
    val currentDate = new Date(timesec)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.setLenient(false)
    val getAge = udf((dob:String) => {
      Try {
        import sprk.implicits._
        val dateUtil = dateFormat.parse(dob)
        val dobDate = new Date(dateUtil.getTime)
        currentDate.getYear - dobDate.getYear
      } match {
        case Success(v) => v.toInt
        case Failure(issue) =>
          throw new ParseException(dob, currentDate.getYear)
          println("Date parse exception -->" + dob)
          0
      }
    })
  
   
 def callCustomerInfo(customerFlat:DataFrame) : DataFrame = {
    
    import sprk.implicits._
    val custInfo = customerFlat.withColumn("age", getAge(col("cust_dob")))
    val resultCustomer = custInfo.withColumn("age_group", when(col("age").>=(15) && col("age").<=(20), "20_age_group")
      .when(col("age").>(21) && col("age").<=(25), "25_age_group")
      .when(col("age").>(25) && col("age").<=(31), "30_age_group")
      .otherwise("unknown"))
    resultCustomer
  } 
  }