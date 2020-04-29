package com.ica.poc.customertest

import com.ica.poc.sessiontest.SessionInitTest
import com.ica.poc.customer.TransactionFlatten
import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.FunSuite
import com.ica.poc.session.SessionInit

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class TransactionFlattenTest extends FunSuite with SessionInitTest {
  
  test("Unit test for transaction dataframe count is not 0") {
    import sprk.implicits._
    val list_input = List(("5001","2","2001","store_01","10001","2500.0","2020-02-15")
        ,("5003","3","3001","store_02","10002","5000.00","2020-02-24")
        ,("5004","2","4001","store_01","10003","3000.00","2020-03-16")
        ,("5009","3","3001","store_02","10002","5000.00","2020-02-20")).toDF("transaction_id","customer_id","product_id","store_id","offer_id","sales","date")
        
    val actual = TransactionFlatten.callCustomerTransaction(list_input)
    
    assert(actual.count() > 0)
  }
  
  test("Unit test for transaction dataframe to check new columns are present or not") {
    import sprk.implicits._
    val list_input = List(("5001","2","2001","store_01","10001","2500.0","2020-02-15")
        ,("5003","3","3001","store_02","10002","5000.00","2020-02-24")
        ,("5004","2","4001","store_01","10003","3000.00","2020-03-16")
        ,("5009","3","3001","store_02","10002","5000.00","2020-02-20")).toDF("transaction_id","customer_id","product_id","store_id","offer_id","sales","date")
        
    val actual = TransactionFlatten.callCustomerTransaction(list_input)
    
    assert(actual.columns.contains("sales_week_number") && actual.columns.contains("day_of_sales_week"))
  }
  
}