package com


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.slf4j.LoggerFactory

object person_xml extends App {
val logger = LoggerFactory.getLogger(this.getClass.getName)
  val spark = SparkSession.builder()

    .appName("person_xml")
    .master("local")
    .getOrCreate()

  Logger.getRootLogger.setLevel(Level.WARN)

  val xmlread = spark.read.format("xml").option("rowTag","person").load("C:\\Users\\Lenovo\\Desktop\\person.xml")
  val gg = xmlread.select(xmlread("age._VALUE").alias("Actul_age"),xmlread("age._born").alias("data_barth"),xmlread("name").alias("person_name"))
      .withColumnRenamed("person_name","name123").withColumn("new_123",lit("true"))
     .withColumn("new_column",when(col("Actul_age")===25,lit("yes")).when(col("Actul_age")===30,lit("No")).otherwise("non") )
      .withColumn("category",when(col("actul_age")>60,"senior citizen").when(col("actul_age")>27,"adult").otherwise("youth"))
gg.show()
  logger.warn("xmlread completed")
  gg.printSchema()
  gg.write.option("header","true").mode("append")csv("C:\\Users\\Lenovo\\Desktop\\b123")
}
