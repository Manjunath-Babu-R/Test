package com


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame

object Json_read extends App {
  val logger= LoggerFactory.getLogger(this.getClass().getName)
  val spark = SparkSession.builder()
    .appName("Json read")
    .master("local")
    .getOrCreate()
  Logger.getRootLogger.setLevel(Level.WARN)
import spark.implicits._
  //val Json_read = spark.read.option("multiline","true").option("startingOffsets","latest").json("C:\\Users\\Lenovo\\Desktop\\IBM1123.json")
  val Json_read_123 = spark.read.option("multiline","true").option("startingOffsets","latest").json("C:\\Users\\Lenovo\\Desktop\\ibm321.json")
  //val uuuu = Json_read.selectExpr("CASTt(value as STRING)","timestamp")
 //val ibm1 = Json_read.select(col("@timestamp"),col("@version"),explode(col("request_http_headers")),col("col.x-fapi-customer-ip-address").as("Customer_ip")).drop("col")
//ibm1.show()
 //ibm1.printSchema()
//uuuu.show()
 //uuuu.printSchema()
  // Json_read.sho  w()
  //Json_read.printSchema()
//input.replaceAll("[&{","")
 val ff:String = "[{'Accept': 'application/json'}, {'X-IBM-Client-Id': '4ddb1d5f7facb187b87187841885cae7'}, {'x-fapi-auth-date': '2019-01-09T15:43:00-08:00'}, {'x-cds-subject': 'Test'}, {'x-fapi-interaction-id': 'Test1234'}, {'x-fapi-customer-ip-address': '10.101.10.0'}, {'x-cds-User-Agent': 'Test'}, {'User-Agent': 'PostmanRuntime/7.24.1'}, {'Postman-Token': 'f1e7f7f1-5dee-4f76-ac64-cfe83e2ab178'}, {'Host': 'apigw.apiccluster-442983326460a9c8264b8ec5b8de42d2-0001.au-syd.containers.appdomain.cloud'}, {'Accept-Encoding': 'gzip, deflate, br'}]"
  val input:String = "[{'x-v': '1'}, {'Accept': 'application/json'}, {'X-IBM-Client-Id': '4ddb1d5f7facb187b87187841885cae7'}, {'x-fapi-auth-date': '2019-01-09T15:43:00-08:00'}, {'x-cds-subject': 'Test'}, {'x-fapi-interaction-id': 'Test1234'}, {'x-fapi-customer-ip-address': '10.101.10.0'}, {'x-cds-User-Agent': 'Test'}, {'User-Agent': 'PostmanRuntime/7.24.1'}, {'Postman-Token': 'f1e7f7f1-5dee-4f76-ac64-cfe83e2ab178'}, {'Host': 'apigw.apiccluster-442983326460a9c8264b8ec5b8de42d2-0001.au-syd.containers.appdomain.cloud'}, {'Accept-Encoding': 'gzip, deflate, br'}]"
  val Replce:String = "[&{"
  val Result =input.replaceAllLiterally(s"$Replce",s"$input")
  val Result1 =input.replaceAllLiterally(s"$input",s"$Replce")
//println("print the result: Replce replced with input:"+Result)
  //println("print the result: input  with Replce:"+Result1)
  val replacements1 = Map( "[&{"->"[{'x-v': '1'}, {'Accept': 'application/json'}, {'X-IBM-Client-Id': '4ddb1d5f7facb187b87187841885cae7'}, {'x-fapi-auth-date': '2019-01-09T15:43:00-08:00'}, {'x-cds-subject': 'Test'}, {'x-fapi-interaction-id': 'Test1234'}, {'x-fapi-customer-ip-address': '10.101.10.0'}, {'x-cds-User-Agent': 'Test'}, {'User-Agent': 'PostmanRuntime/7.24.1'}, {'Postman-Token': 'f1e7f7f1-5dee-4f76-ac64-cfe83e2ab178'}, {'Host': 'apigw.apiccluster-442983326460a9c8264b8ec5b8de42d2-0001.au-syd.containers.appdomain.cloud'}, {'Accept-Encoding': 'gzip, deflate, br'}]")
  val replacements = Map( "[{'x-v': '1'}, {'Accept': 'application/json'}, {'X-IBM-Client-Id': '4ddb1d5f7facb187b87187841885cae7'}, {'x-fapi-auth-date': '2019-01-09T15:43:00-08:00'}, {'x-cds-subject': 'Test'}, {'x-fapi-interaction-id': 'Test1234'}, {'x-fapi-customer-ip-address': '10.101.10.0'}, {'x-cds-User-Agent': 'Test'}, {'User-Agent': 'PostmanRuntime/7.24.1'}, {'Postman-Token': 'f1e7f7f1-5dee-4f76-ac64-cfe83e2ab178'}, {'Host': 'apigw.apiccluster-442983326460a9c8264b8ec5b8de42d2-0001.au-syd.containers.appdomain.cloud'}, {'Accept-Encoding': 'gzip, deflate, br'}]" -> "[&{")
  val str = "[{'x-v': '1'}, {'Accept': 'application/json'}, {'X-IBM-Client-Id': '4ddb1d5f7facb187b87187841885cae7'}, {'x-fapi-auth-date': '2019-01-09T15:43:00-08:00'}, {'x-cds-subject': 'Test'}, {'x-fapi-interaction-id': 'Test1234'}, {'x-fapi-customer-ip-address': '10.101.10.0'}, {'x-cds-User-Agent': 'Test'}, {'User-Agent': 'PostmanRuntime/7.24.1'}, {'Postman-Token': 'f1e7f7f1-5dee-4f76-ac64-cfe83e2ab178'}, {'Host': 'apigw.apiccluster-442983326460a9c8264b8ec5b8de42d2-0001.au-syd.containers.appdomain.cloud'}, {'Accept-Encoding': 'gzip, deflate, br'}]"
  val result=replacements.foldLeft(str)((a, b) => a.replaceAllLiterally(b._1, b._2))
  val result1=replacements1.foldLeft(str)((a, b) => a.replaceAllLiterally(b._1, b._2))
  println(result)
  println(result1)


  //input.foldLeft(Replce)((a,b)=>a.replaceAllLiterally(b._))

 //val rsul1 ="[{'x-v':'1'}, {'Accept':'application/json'},{'X-IBM-Client-Id':'4ddb1d5f7facb187b87187841885cae7'},{'x-fapi-auth-date': '2019-01-09T15:43:00-08:00'}, {'x-cds-subject': 'Test'}, {'x-fapi-interaction-id': 'Test1234'}, {'x-fapi-customer-ip-address': '10.101.10.0'}, {'x-cds-User-Agent': 'Test'}, {'User-Agent': 'PostmanRuntime/7.24.1'}, {'Postman-Token': 'f1e7f7f1-5dee-4f76-ac64-cfe83e2ab178'}, {'Host': 'apigw.apiccluster-442983326460a9c8264b8ec5b8de42d2-0001.au-syd.containers.appdomain.cloud'},{'Accept-Encoding': 'gzip, deflate, br'}]".replaceAll("[ {'Accept':'application/json'},{'X-IBM-Client-Id':'4ddb1d5f7facb187b87187841885cae7'},{'x-fapi-auth-date': '2019-01-09T15:43:00-08:00'}, {'x-cds-subject': 'Test'}, {'x-fapi-interaction-id': 'Test1234'}, {'x-fapi-customer-ip-address': '10.101.10.0'}, {'x-cds-User-Agent': 'Test'}, {'User-Agent': 'PostmanRuntime/7.24.1'}, {'Postman-Token': 'f1e7f7f1-5dee-4f76-ac64-cfe83e2ab178'}, {'Host': 'apigw.apiccluster-442983326460a9c8264b8ec5b8de42d2-0001.au-syd.containers.appdomain.cloud'},{'Accept-Encoding': 'gzip, deflate, br'}]", "[&{")

  //val rsul ="mamanju".replaceAll("man", "nju")
  //val list1 = String("[{'x-v':'1'}, {'Accept':'application/json'},{'X-IBM-Client-Id':'4ddb1d5f7facb187b87187841885cae7'},{'x-fapi-auth-date': '2019-01-09T15:43:00-08:00'}, {'x-cds-subject': 'Test'}, {'x-fapi-interaction-id': 'Test1234'}, {'x-fapi-customer-ip-address': '10.101.10.0'}, {'x-cds-User-Agent': 'Test'}, {'User-Agent': 'PostmanRuntime/7.24.1'}, {'Postman-Token': 'f1e7f7f1-5dee-4f76-ac64-cfe83e2ab178'}, {'Host': 'apigw.apiccluster-442983326460a9c8264b8ec5b8de42d2-0001.au-syd.containers.appdomain.cloud'},{'Accept-Encoding': 'gzip, deflate, br'}]")
  //val h= list1.replaceAll("[&{",$"[{'x-v':'1'}, {'Accept':'application/json'},{'X-IBM-Client-Id':'4ddb1d5f7facb187b87187841885cae7'},{'x-fapi-auth-date': '2019-01-09T15:43:00-08:00'}, {'x-cds-subject': 'Test'}, {'x-fapi-interaction-id': 'Test1234'}, {'x-fapi-customer-ip-address': '10.101.10.0'}, {'x-cds-User-Agent': 'Test'}, {'User-Agent': 'PostmanRuntime/7.24.1'}, {'Postman-Token': 'f1e7f7f1-5dee-4f76-ac64-cfe83e2ab178'}, {'Host': 'apigw.apiccluster-442983326460a9c8264b8ec5b8de42d2-0001.au-syd.containers.appdomain.cloud'},{'Accept-Encoding': 'gzip, deflate, br'}]")
  //Json_read_123.show()
  //Json_read_123.printSchema()
}
