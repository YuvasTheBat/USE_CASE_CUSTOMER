package com.ica.poc.customertest

import com.ica.poc.sessiontest.SessionInitTest
import com.ica.poc.customer.CustomerInfoFlatten
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.FunSuite
import org.scalatest.Assertions.assertTypeError
import org.scalatest.Assertions.assertThrows
import com.ica.poc.session.SessionInit
import org.apache.spark.SparkException

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.text.ParseException

@RunWith(classOf[JUnitRunner])
class CustomerInfoFlattenTest extends FunSuite with SessionInitTest {
  
  test("Unit test for compare actual and expected dataframes") {
    import sprk.implicits._
    val list_input = List(("1", "yuvas", "1989-06-22", "M"), ("2", "murugan", "1989-09-23", "M"))
    val input = sprk.sparkContext.parallelize(list_input).toDF("cust_id", "cust_name", "cust_dob", "gender")
    val expected = List(("1", "yuvas", "1989-06-22", "M", 31, "30_age_group"),
        ("2", "murugan", "1989-09-23", "M", 31, "30_age_group"))
      .toDF("cust_id", "cust_name", "cust_dob", "gender", "age", "age_group")
    val actual = CustomerInfoFlatten.callCustomerInfo(input)
    val newDf = actual.except(expected)
    newDf.show()
    assert(newDf.rdd.isEmpty())
  }
  
  test("Fail Scenario : Unit test for compare actual and expected dataframes") {
    import sprk.implicits._
    val list_input = List(("1", "yuvas", "1989-06-22", "M"), ("2", "murugan", "1989-09-23", "M"))
    val input = sprk.sparkContext.parallelize(list_input).toDF("cust_id", "cust_name", "cust_dob", "gender")
    val expected = List(("1", "yuvas", "1989-06-22", "M", 31, "30_age_group"), ("2", "murugan", "1989-09-23", "M", 31, "30_age_group"),
        ("3","raja","1990-05-24","M",30,"30_age_group"))
      .toDF("cust_id", "cust_name", "cust_dob", "gender", "age", "age_group")
    val actual = CustomerInfoFlatten.callCustomerInfo(input)
    val newDf = expected.except(actual)
    assert(!newDf.rdd.isEmpty())
  }
  
  test("Unit testing for check number of columns in dataframe") {

    import sprk.implicits._
    val list_input = List(("1", "yuvas", "1989-06-22", "M"), ("2", "murugan", "1989-09-23", "M"), ("3", "kamali", "1992-06-24", "F"))
    val input = sprk.sparkContext.parallelize(list_input).toDF("cust_id", "cust_name", "cust_dob", "gender")
    val actual = CustomerInfoFlatten.callCustomerInfo(input)
    assert(actual.columns.size == 6)
  }

  test("Unit testing for check new columns are in actual dataframe or not") {
    import sprk.implicits._
    val list_input = List(("1", "yuvas", "1989-06-22", "M"), ("2", "murugan", "1989-09-23", "M"), ("3", "kamali", "1992-06-24", "F"))
    val input = sprk.sparkContext.parallelize(list_input).toDF("cust_id", "cust_name", "cust_dob", "gender")
    val actual = CustomerInfoFlatten.callCustomerInfo(input)
    assert(actual.columns.contains("age") && actual.columns.contains("age_group"))
  }

  test ("Unit test case for test ParseException while passing wrong date format") {
    import sprk.implicits._
    val list_input = List(("1", "yuvas", "24-06-1992", "M"))
    val input = sprk.sparkContext.parallelize(list_input).toDF("cust_id", "cust_name", "cust_dob", "gender")
    val thrown = intercept[SparkException] {
      val ageFind = input.withColumn("AGE", CustomerInfoFlatten.getAge(input.col("cust_dob")))
      ageFind.show()
    }
    assert(thrown.getMessage.contains("ParseException"))
  }
}