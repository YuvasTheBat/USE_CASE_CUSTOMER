package com.ica.poc.session

import org.apache.spark.sql.SparkSession

/*
 * @author Yuvaraj Mani
 * @version 1.0
 * This trait is used to intialize spark session in other modules.
 */

trait SessionInit {
  
   implicit  val sprk = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
}