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
    
    //Flatten_Customer_Info
    val getCustomerInfoDataFrame = CustomerInfoFlatten.getCustomerInfoDataFrame(sprk)
    val customer_call = CustomerInfoFlatten.callCustomerInfo(getCustomerInfoDataFrame)
    
    //Flatten_Customer_Offer
    val getCustomerOfferDataFrame = CustomerOfferFlatten.getCustomerOfferDataFrame(sprk)
    val getWeeksOfYear = CustomerOfferFlatten.getWeeksOfYear(sprk)
    val customer_offer_call = CustomerOfferFlatten.customerOfferCall(getCustomerOfferDataFrame,getWeeksOfYear)

    //Flatten_Customer_Transaction
    val getTransactionDataFrame = TransactionFlatten.getTransactionDataFrame(sprk)
    val transaction = TransactionFlatten.callCustomerTransaction(getTransactionDataFrame)
    val transaction_res = CustomerUtilities.transaction_res(transaction,customer_call)

    //Fetching_Customers_Weekly_Status
    val customerWiseweeklyStatus = CustomerUtilities.customerWiseweeklyStatus(customer_call, customer_offer_call, transaction)

    customerWiseweeklyStatus.show(10)
    
    //Fetching_OverAll_Weekly_Status
    val customerOverAllWeeklyStatus = CustomerUtilities.customerOverAllWeeklyStatus(customerWiseweeklyStatus)

    customerOverAllWeeklyStatus.show(10)

    //Fetching_AgeGroup_Transaction
    val transactionOnAgeGroupWise = CustomerUtilities.transactionOnAgeGroupWise(transaction_res)
    
    transactionOnAgeGroupWise.show(10)
    
  }
  
}