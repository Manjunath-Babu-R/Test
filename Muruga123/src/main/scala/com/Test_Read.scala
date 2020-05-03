package com

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Test_Read extends App {

  val spark = SparkSession.builder()
    .appName("Trxt")
    .master("local")
    .getOrCreate()
  import spark.implicits._
  val test_read1 = spark.sparkContext.textFile("C:\\Users\\Lenovo\\Desktop\\test123.txt")
  val mapp = test_read1.map(t => {
    val r = t.split(",")
    Row(r(0).trim.toInt,r(1),r(2).trim.toInt)
    })
  case class Fru (ID:Int,Name:String,Qury:Int)
  val schema123 = StructType(Array(StructField("ID",IntegerType,true),
    StructField("Name",StringType,true),
    StructField("Fruites",IntegerType,true)
    ))

  val Df1 = spark.createDataFrame(mapp,schema123).show()


 //val  Df123 = test_read1.collect().foreach(println(_))

}
