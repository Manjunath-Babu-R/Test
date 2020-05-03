package com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.Inner
import  org.apache.spark.sql.functions._

object Join_sales extends App {

  val spark = SparkSession.builder()
    .appName("Join")
    .master("local")
    .getOrCreate()

  val sleas_DF = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\Lenovo\\Desktop\\sample1.csv")
  val cust_DF = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\Lenovo\\Desktop\\Book1.csv")
  val Join_sl_cu_DF = cust_DF.join(sleas_DF,Seq("transactionId"),"full")
    val join_sl_cu_DF1= cust_DF.join(sleas_DF,cust_DF("transactionId")===sleas_DF("transactionId"),"left")
val Drop_join = join_sl_cu_DF1.drop(sleas_DF("transactionId"))
  .groupBy("transactionId","Name","customerId","temId","amountPaid")
    .agg(sum("amountPaid").alias("tatol_amount"))
  val test_gro =join_sl_cu_DF1.drop(sleas_DF("transactionId"))
    .groupBy("customerId")
    .agg(sum("amountPaid").alias("tatol_amount"))
  val na2 = Drop_join.na.drop(Seq("temId","amountPaid"))
  val na3=Drop_join.na.fill(20,Seq("temId","amountPaid"))
  val na4=na3.na.replace("Name",Map("Babu"->"Babu123","Naha"->"Neha123","Mhona"->"Mohan Babu"))
  //Drop_join.show()
na4.limit(5).dropDuplicates(Seq("customerId","temId")).describe("temId").show()


}
