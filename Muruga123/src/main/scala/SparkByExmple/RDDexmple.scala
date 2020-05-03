package SparkByExmple

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j._

import org.apache.log4j.Level

object RDDexmple extends App {
val logger = LoggerFactory.getLogger(this.getClass.getName)
  val spark = SparkSession.builder()
    .appName("RDD exmple")
    .master("local")
    .getOrCreate()
  Logger.getRootLogger.setLevel(Level.WARN)


  val rdd = spark.sparkContext.textFile("C:\\Users\\Lenovo\\Desktop\\Muruga\\*,C:\\Users\\Lenovo\\Desktop\\Muruga\\text01.txt")
  rdd.collect().foreach(println(_))
  val map1 = rdd.map(f => f.split(","))
  map1.foreach(f => {println("col1:"+f(0)+",col2:"+f(1))})



}
