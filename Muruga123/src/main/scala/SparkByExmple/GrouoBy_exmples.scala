package SparkByExmple

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level,Logger}
import  org.slf4j.LoggerFactory
object GrouoBy_exmples extends  App {

  val spark = SparkSession.builder()
    .appName("groupby")
    .master("local")
    .getOrCreate()
  val log = LoggerFactory.getLogger(this.getClass.getName)
Logger.getRootLogger.setLevel(Level.WARN)

  import spark.implicits._
  val simpleData = Seq(("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000))
  val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
  df.show()
  val deopdu =df.dropDuplicates("department","state")
  deopdu.show()
  val df1 = df.groupBy("department").sum("salary")
  df1.show()
  val df2 = df.groupBy("department","state").sum("salary","bonus")
  df2.show()
  import org.apache.spark.sql.functions._
  val df3 =df.groupBy("department").agg(sum("salary").as("sum_salary"),
    avg("salary").as("avg_salary"),
    min("salary").as("min_salary"),
    max("bonus").as("max_bouns"),
    sum("bonus").as("sum_bonus")).where($"sum_bonus">= 50000)

  df3.show()
}
