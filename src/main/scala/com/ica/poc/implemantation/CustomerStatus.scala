package com.ica.poc.implemantation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import com.ica.poc.session.SessionInit
import com.ica.poc.customer.CustomerInfoFlatten
import com.ica.poc.customer.CustomerOfferFlatten
import com.ica.poc.customer.TransactionFlatten
import com.ica.poc.utilities.CustomerUtilities
import org.apache.spark.sql.types.DoubleType

/*
 * @author Yuvaraj Mani
 * @version 1.0
 * This object is used to run the functionalities one by one.
 * Exception handling takes care in calling methods.
 */

object CustomerStatus extends SessionInit{
  
   def main(args:Array[String]) {
    
    /** Flatten Customer Info */
    val getCustomerInfoDataFrame = CustomerInfoFlatten.getCustomerInfoDataFrame(sprk)
    val customerCall = CustomerInfoFlatten.callCustomerInfo(getCustomerInfoDataFrame)
    
    /** Flatten Customer Offer Info */
    val getCustomerOfferDataFrame = CustomerOfferFlatten.getCustomerOfferDataFrame(sprk)
    val getWeeksOfYear = CustomerOfferFlatten.getWeeksOfYear(sprk)
    val customerOfferCall = CustomerOfferFlatten.customerOfferCall(getCustomerOfferDataFrame,getWeeksOfYear)
    
    /** Flatten Customer's Transaction */
    val getTransactionDataFrame = TransactionFlatten.getTransactionDataFrame(sprk)
    val transaction = TransactionFlatten.callCustomerTransaction(getTransactionDataFrame)
    val customerTransaction = CustomerUtilities.customerTransactionResult(transaction,customerCall)
    
   /** Fetching each customer's weekly status */
    val customerWiseweeklyStatus = CustomerUtilities.customerWiseweeklyStatus(customerCall, customerOfferCall, transaction)
    customerWiseweeklyStatus.show(10)
    
    /** Fetching overall weekly status */
    val customerOverAllWeeklyStatus = CustomerUtilities.customerOverAllWeeklyStatus(customerWiseweeklyStatus)
    customerOverAllWeeklyStatus.show(10)

    /** Fetching customer's age group wise details */
    val transactionOnAgeGroupWise = CustomerUtilities.transactionOnAgeGroupWise(customerTransaction)
    transactionOnAgeGroupWise.show(10)
  }
  
}