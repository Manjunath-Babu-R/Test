package com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UDF_code extends  App {

  val spark = SparkSession.builder()
    .appName("UDF_code")
    .master("local")
    .getOrCreate()
  val read = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\Lenovo\\Desktop\\sample1.csv")
  val ggh =udf((X:Float)=>X+6)
  val df2 = read.withColumn("in_price",ggh(col("amountPaid")))
df2.show()
}
