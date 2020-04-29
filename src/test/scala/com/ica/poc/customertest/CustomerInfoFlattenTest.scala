package com.ica.poc.customertest

import com.ica.poc.sessiontest.SessionInitTest
import com.ica.poc.customer.CustomerInfoFlatten
import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.FunSuite
import com.ica.poc.session.SessionInit

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
@RunWith(classOf[JUnitRunner])
class CustomerInfoFlattenTest extends FunSuite with SessionInitTest {
  
  test("Unit test for check count of dataframe is not zero") {
    
    import sprk.implicits._
    val list_input = List(("1","yuvas","1989-06-22","M"),("2","murugan","1989-09-23","M"),("3","kamali","1992-06-24","F"))
    
    val input = sprk.sparkContext.parallelize(list_input).toDF("cust_id","cust_name","cust_dob","gender")
    
    val expected = List(("1","yuvas","1989-06-22","M",31,"30_age_group")
                        ,("2","murugan","1989-09-23","M",31,"30_age_group")
                        ,("3","kamali","1992-06-25","F",28,"30_age_group"))
                        .toDF("cust_id","cust_name","cust_dob","gender","age","age_group")
                        
    val actual = CustomerInfoFlatten.callCustomerInfo(input)
    
    assert(actual.count() > 0)
  }
  
  test("Unit testing for check number of columns in dataframe") {
    
    import sprk.implicits._
    val list_input = List(("1","yuvas","1989-06-22","M"),("2","murugan","1989-09-23","M"),("3","kamali","1992-06-24","F"))
    
    val input = sprk.sparkContext.parallelize(list_input).toDF("cust_id","cust_name","cust_dob","gender")
    val actual = CustomerInfoFlatten.callCustomerInfo(input)
    
    assert(actual.columns.size == 6)
  }

  test("Unit testing for check new columns are in actual dataframe or not") {
    import sprk.implicits._
    val list_input = List(("1","yuvas","1989-06-22","M"),("2","murugan","1989-09-23","M"),("3","kamali","1992-06-24","F"))
    
    val input = sprk.sparkContext.parallelize(list_input).toDF("cust_id","cust_name","cust_dob","gender")
    val actual = CustomerInfoFlatten.callCustomerInfo(input)
    
    assert(actual.columns.contains("age") && actual.columns.contains("age_group"))
  }
}