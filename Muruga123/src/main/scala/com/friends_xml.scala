package com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object friends_xml extends App {

  val spark = SparkSession.builder()
    .appName("frinds_xml")
    .master("local")
    .getOrCreate()

  val read = spark.read.option("rowTag","string-array").format("xml").load("C:\\Users\\Lenovo\\Desktop\\perr123.xml")
      .withColumn("item",explode(col("item"))).withColumnRenamed("_name","Languages_Name")
      .withColumn("derive_colum",when(col("item")==="English",lit("India")).when(col("item")==="French",lit("paris")).otherwise("other_countrys"))
      .withColumn("row_number",row_number().over(Window.partitionBy("Languages_Name").orderBy("item")))
      .withColumn("rank",rank().over(Window.partitionBy("Languages_Name").orderBy("derive_colum")))
      .withColumn("dense_rank",dense_rank().over(Window.partitionBy("Languages_Name").orderBy("derive_colum")))
      .withColumn("pertange_rank",percent_rank().over(Window.partitionBy("Languages_Name").orderBy("derive_colum")))
      .withColumn("lag",lag("rank",1).over(Window.partitionBy("Languages_Name").orderBy("derive_colum")))
      .withColumn("lead",lead("rank",1).over(Window.partitionBy("Languages_Name").orderBy("derive_colum")))
      .withColumn("sum",sum("rank").over(Window.partitionBy("Languages_Name")))
      .groupBy("rank","lag","item").agg(avg("sum").alias("avg")).filter(col("item")==="English" || col("item")==="French" )
  read.show()
  read.printSchema()

}
