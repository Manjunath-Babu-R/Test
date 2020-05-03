package SparkByExmple

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

class Rename_colimns  {
  val spark = SparkSession.builder()
    .appName("Rename colums")
    .master("local")
    .getOrCreate()
  val logger = LoggerFactory.getLogger(this.getClass.getName)
  Logger.getRootLogger.setLevel(Level.WARN)

  val data = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
    Row(Row("Michael ","Rose",""),"40288","M",4000),
    Row(Row("Robert ","","Williams"),"42114","M",4000),
    Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
    Row(Row("Jen","Mary","Brown"),"","F",-1))

  val schema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("dob",StringType)
    .add("gender",StringType)
    .add("salary",IntegerType)

  val DF = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
  DF.show()

  object rename extends App {
    val obj = new Rename_colimns
  }
}
