package com.ica.poc.utilitiestest

import com.ica.poc.sessiontest.SessionInitTest
import com.ica.poc.utilities.CustomerUtilities
import org.scalatest
import org.scalatest.FunSuite
import org.apache.spark.sql.DataFrame

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
case class emptyCustomer(cust_id: String, cust_name:String,cust_dob:String,gender:String,age: Int,age_group:String)

class CustomerUtilitiesTest extends FunSuite with SessionInitTest {

  import sprk.implicits._
  val customer: DataFrame = null

  val customerEmpty = sprk.emptyDataset[emptyCustomer].toDF()

  val transactionData = List(("5001", "2", "2001", "store_01", "10001", "2500.0", "2020-02-15", 7, "Sat", "30_age_group"), ("5003", "3", "3001", "store_02", "10002", "5000.00", "2020-02-24", 9, "Mon", "30_age_group"))
    .toDF("transaction_id", "customer_id", "product_id", "store_id", "offer_id", "sales", "date", "sales_week_number", "day_of_sales_week", "age_group")
  
  val transactionDataNull:DataFrame = null
  val customerCall = List(("1", "yuvas", "1989-06-22", "M", 31, "30_age_group"), ("2", "murugan", "1989-09-23", "M", 31, "30_age_group"), ("3", "kamali", "1992-06-25", "F", 28, "30_age_group"))
    .toDF("cust_id", "cust_name", "cust_dob", "gender", "age", "age_group")

  val customerOfferCall = List(("6", "10001", "winter_sale", "2", "2020-02-02", "2020-02-28", "6", "9"), ("7", "10001", "winter_sale", "2", "2020-02-02", "2020-02-28", "6", "9"), ("8", "10001", "winter_sale", "2", "2020-02-02", "2020-02-28", "6", "9"), ("6", "10002", "second_sale", "3", "2020-02-10", "2020-02-25", "6", "7"), ("7", "10002", "second_sale", "3", "2020-02-10", "2020-02-25", "6", "7"))
    .toDF("week_number", "offer_id", "offer_name", "cust_id", "begin_date", "end_date", "begin_week_number", "end_week_number")

  val transaction = List(("5001", "2", "2001", "store_01", "10001", "2500.0", "2020-02-15", 7, "Sat"), ("5003", "3", "3001", "store_02", "10002", "5000.00", "2020-02-24", 9, "Mon"), ("5004", "2", "4001", "store_01", "10003", "3000.00", "2020-03-16", 12, "Mon"), ("5009", "3", "3001", "store_02", "10002", "5000.00", "2020-02-20", 8, "Thu"))
    .toDF("transaction_id", "customer_id", "product_id", "store_id", "offer_id", "sales", "date", "sales_week_number", "day_of_sales_week")

  test("Unit test case to check whether customer weekwise status dataframe count is not zero") {
    import sprk.implicits._
    println("Is empty check--->" + customerEmpty.rdd.isEmpty())
    val actual = CustomerUtilities.customerWiseweeklyStatus(customerCall, customerOfferCall, transaction)
    assert(actual.count() > 0)
  }
  test("Unit test case to check number of columns is expected") {
    import sprk.implicits._
    val actual = CustomerUtilities.customerWiseweeklyStatus(customerCall, customerOfferCall, transaction)
    assert(actual.columns.size == 8)
  }
  test("Unit test case to find whether customer weekwise status dataframe has new columns in it or not") {
    import sprk.implicits._
    val actual = CustomerUtilities.customerWiseweeklyStatus(customerCall, customerOfferCall, transaction)
    assert(actual.columns.contains("offer_received_count") && actual.columns.contains("offer_redeemed_count")
      && actual.columns.contains("no_of_visit"))
  }
  test("Unit test case for transaction data") {
    import sprk.implicits._
    val thrown = intercept[NullPointerException] {
      val ageFind = CustomerUtilities.transactionOnAgeGroupWise(transactionDataNull)
      ageFind.show()
    }
    assert(thrown.getMessage.contains("transactionAgeGroup is null"))
  }
}