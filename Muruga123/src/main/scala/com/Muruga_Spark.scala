package com

import javafx.scene.input.Mnemonic
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
object Muruga_Spark  extends App {

  val spark = SparkSession.builder()
    .appName("frist spark  code")
    .master("local")
    .getOrCreate()

  println("hello spark")
 val rdd = spark.read.option("header","true").csv("C:\\SG\\SG\\src\\main\\resources\\sample.csv")
rdd.show()
  //rdd.createOrReplaceTempView("SampleTable")

  val gg = Window.partitionBy("sensor","Mnemonic").orderBy("timestamp")
  val expt_ouput_df =rdd.withColumn("next_data",lag(col("data"),1).over(gg))
    .withColumn("next_timeStmap",lag(col("timestamp"),1).over(gg))
    .filter(col("data") =!= col("next_data"))
    .select("sensor","Mnemonic","data","next_data","timestamp","next_timeStmap")

  expt_ouput_df.show()

/*
  val r= s"""select sensor, Mnemonic, data, next_data, timestamp, next_timeStamp
      |from
      |( select *,
      |LEAD(data) OVER( PARTITION BY sensor, Mnemonic ORDER BY timestamp) AS next_data,
      |LEAD(timestamp) OVER( PARTITION BY sensor, Mnemonic ORDER BY timestamp) AS next_timeStamp
      |from SampleTable
      |) a
      | where data <> next_data""".stripMargin

  val tt = spark.sql(r)

  tt.show()

  rdd.show()*/
}
