package com.ishumei.poc.utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def getSpark(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local")
      .config("spark.debug.maxToStringFields", "200")
      .getOrCreate()
//    System.setProperty("hadoop.home.dir", "C:\\files\\googledown\\winutils-master\\hadoop-2.8.3\\bin")

    spark
  }

}
