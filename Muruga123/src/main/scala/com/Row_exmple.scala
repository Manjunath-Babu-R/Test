package com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
object Row_exmple extends  App {

  val spark = SparkSession.builder()
    .appName("row code")
    .master("local")
    .getOrCreate()
  val RDD = spark.sparkContext.textFile("C:\\Users\\Lenovo\\Desktop\\sample1.csv")
  //val fg = RDD.filter(g => g != RDD.first())
  val fg1 = RDD.filter(h => !h.startsWith("transactionId"))
  val hhjj =fg1.map(rec =>{
    val arr =rec.split(",")
    Row(arr(0).toInt,arr(1).toInt,arr(2).toInt,arr(3).toDouble)
  })
  val schema1 = StructType(Array(StructField("trstionid",IntegerType,true),
    StructField("custid",IntegerType,true),
    StructField("itramid",IntegerType,true),
    StructField("amountpaid",DoubleType,true)
  ))
  val df =spark.createDataFrame(hhjj,schema1)
  //hhjj.collect().foreach(println(_))
  df.show()
  df.printSchema()
  println("print describe")
  df.describe("amountpaid").show()
  println("print distinct")
  df.distinct()
  println("print drop duplicut")
  val tt=df.dropDuplicates("custid","itramid")
  tt.show()
  println("print drop one colunm")
  val df1 =df.drop("itramid")
  df1.show()



}
