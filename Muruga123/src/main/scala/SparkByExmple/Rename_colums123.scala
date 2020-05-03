package SparkByExmple

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._

object Rename_colums123 extends App {
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
      .withColumnRenamed("dob","Date Birth")
      .withColumnRenamed("salary","salary_Amount")

  val schema1 = new StructType()
    .add("fname",StringType)
    .add("middlaname",StringType)
    .add("LastName",StringType)
  DF.select(col("name").cast(schema1),col("Date Birth"),col("gender"),col("salary_Amount"))
  DF.show()
  DF.printSchema()
  val DF1=DF.select(col("name.firstname").as("Fist_Name"),col("name.middlename").alias("Middle_Name"),col("name.lastname").as("Last_Name"),col("Date Birth"),col("gender"),col("salary_Amount"))
 DF1.show()
  DF1.printSchema()

  val DF4 =DF.withColumn("Fist_Name123",col("name.firstname"))
    .withColumn("Middle_Name123",col("name.middlename"))
    .withColumn("Last_Name123",col("name.lastname")).drop("name")

  DF4.show()
  val old_columns = Seq("Date Birth","gender","salary_Amount","Fist_Name123","Middle_Name123","Last_Name123")
  val new_columns = Seq("DateOfBirth","Sex","salary","firstName","middleName","lastName")
  val columnsList = old_columns.zip(new_columns).map(f=>{col(f._1).as(f._2)})
  println(columnsList)
  val df5 = DF4.select(columnsList:_*)
  df5.show()
  df5.printSchema()

}
