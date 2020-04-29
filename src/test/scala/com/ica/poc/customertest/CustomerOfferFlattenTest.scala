package com.ica.poc.customertest

import com.ica.poc.sessiontest.SessionInitTest
import com.ica.poc.customer.CustomerOfferFlatten
import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.FunSuite
import com.ica.poc.session.SessionInit

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
@RunWith(classOf[JUnitRunner])
class CustomerOfferFlattenTest extends FunSuite with SessionInitTest {
  
  test("Test case for check customer offer dataframe is not empty") {
    import sprk.implicits._
    val customer_offer = List(("10001","winter_sale","2","2020-02-02","2020-02-28")
        ,("10001","winter_sale","3","2020-02-02","2020-02-28")
        ,("10001","winter_sale","4","2020-02-02","2020-02-28")
        ,("10002","second_sale","3","2020-02-10","2020-02-25")
        ,("10002","second_sale","5","2020-02-10","2020-02-25")).toDF("offer_id","offer_name","cust_id","begin_date","end_date")
    val weekly = List(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
        ,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41
        ,42,43,44,45,46,47,48,49,50,51,52,53).toDF("week_number")
        
    val actual = CustomerOfferFlatten.customerOfferCall(customer_offer, weekly)
    
    assert(actual.count() > 0)
    assert(actual.columns.size == 8)
  }
  
  test("Test case for check customer offer dataframe contains new columns after computation") {
    import sprk.implicits._
    val customer_offer = List(("10001","winter_sale","2","2020-02-02","2020-02-28")
        ,("10001","winter_sale","3","2020-02-02","2020-02-28")
        ,("10001","winter_sale","4","2020-02-02","2020-02-28")
        ,("10002","second_sale","3","2020-02-10","2020-02-25")
        ,("10002","second_sale","5","2020-02-10","2020-02-25")).toDF("offer_id","offer_name","cust_id","begin_date","end_date")
    val weekly = List(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
        ,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41
        ,42,43,44,45,46,47,48,49,50,51,52,53).toDF("week_number")
        
    val actual = CustomerOfferFlatten.customerOfferCall(customer_offer, weekly)
    
    assert(actual.columns.contains("begin_week_number") && actual.columns.contains("end_week_number"))
  }
}