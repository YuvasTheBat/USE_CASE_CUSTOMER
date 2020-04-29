package com.ica.poc.sessiontest

import org.apache.spark.sql.SparkSession

trait SessionInitTest {
  
   implicit  val sprk = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
}