package com

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.apache.log4j.Logger
import org.apache.log4j.Level

object sales_csv extends  App {
  val log = LoggerFactory.getLogger(this.getClass.getName)
  val spark = SparkSession.builder()
    .appName("slaes")
    .master("local")
    .getOrCreate()
  Logger.getRootLogger.setLevel(Level.WARN)
val optionMap = Map("header"->"true","inferSchema"->"true")
val read_csv= spark.read.options(optionMap).csv("C:\\Users\\Lenovo\\Desktop\\sample1.csv")
  log.warn("CSV file reded")

read_csv.createOrReplaceTempView("selas")

  val tt = spark.sql("select transactionId,customerId,temId,amountPaid from selas A inner join(select  temId as temID1,max(amountPaid) as amountpais123 from selas group by temID1) B on A.temID=B.temID1 and A.amountPaid=B.amountpais123")
  tt.show()

  val rr = spark.sql("select  temId as temID1,max(amountPaid) as amountpais123 from selas group by temID1").show()

  //val ff = spark.sql(s"from sleas select customerId,temId,amountPaid where temId=2").show()
  //read_csv.show()
 // read_csv.printSchema()
  //read_csv.select("transactionID","customerId").where("customerId==2").show
  //println("Total numaber records:"+ read_csv.count())

}
