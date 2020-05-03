package SparkByExmple

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.slf4j.LoggerFactory

object Spark_SQL_exmples extends  App {

 val  spark = SparkSession.builder()
   .appName("spark sql exmples")
   .master("local")
   .getOrCreate()
val logger = LoggerFactory.getLogger(this.getClass.getName)
  Logger.getRootLogger.setLevel(Level.WARN)
  import spark.implicits._
  val columns = Seq("language","users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
  val rdd = spark.sparkContext.parallelize(data).toDF(columns:_*)
  rdd.show()
  rdd.printSchema()
  var datafro =spark.createDataFrame(data).toDF(columns:_*)
datafro.show()
}
