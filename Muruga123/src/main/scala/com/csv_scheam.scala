package com



import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory
import org.apache.log4j.Level
import org.apache.log4j.Logger


object csv_scheam extends  App {
  val log = LoggerFactory.getLogger(this.getClass.getName)
  val spark = SparkSession.builder()
    .appName("structtype")
    .master("local")
    .config("spark.sql.warehouse.dir","/apps/hivewarehose")
    .config("spark.hive.exec.dynamic.partition","true")
    .config("spark.hive.exec.dynomic.partition.mode","true")
    .enableHiveSupport()
    .getOrCreate()
Logger.getRootLogger.setLevel(Level.WARN)
  val schema1 = StructType(Array(
    StructField("transactionId", LongType, true),
    StructField("customerId", LongType, true),
    StructField("temId", LongType, true),
    StructField("amountPaid", DoubleType, true)
  ))
  case class sl(transactionId:Int,customerId:Int,temId:Int,amountPaid:Double)
  import spark.implicits._
  val ttt =s"01-03-2019"
  val read = spark.read.option("header", "true").option("inferSchema", "true").csv("C:\\Users\\Lenovo\\Desktop\\sample1.csv")
      .as[sl]
  read.createOrReplaceTempView("CSV_tmp")
  log.warn("CSV file reading is completed")
  read.show()
  //val fil = read.filter(f => f !=read.first())
  /*
  val rowRDD = fil.map(r => {
val colArr =r.split(" ")
    Row(colArr(0).toInt,colArr(1).toInt,colArr(2).toInt,colArr(3).toDouble)
  })
*/
}