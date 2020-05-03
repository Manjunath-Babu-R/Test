package SparkByExmple

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.apache.log4j.{Logger,Level}
import org.apache.spark.sql.functions._

object otherwise_case extends App {

  val spark = SparkSession.builder()
    .appName("case")
    .master("local")
    .getOrCreate()
  val log =LoggerFactory.getLogger(this.getClass.getName)
  Logger.getRootLogger.setLevel(Level.WARN)

  val data = List(("James","","Smith","36636","M",60000),
    ("Michael","Rose","","40288","M",70000),
    ("Robert","","Williams","42114","",400000),
    ("Maria","Anne","Jones","39192","F",500000),
    ("Jen","Mary","Brown","","F",0))
  val cols = Seq("first_name","middle_name","last_name","dob","gender","salary")

  val DF = spark.createDataFrame(spark.sparkContext.parallelize(data)).toDF(cols:_*)
      .withColumn("new_gender",when(col("gender")=== "M","Male").when(col("gender")==="F","Female").otherwise("unkown"))
  DF.show()
  val df3 = DF.withColumn("new_gender1",
    expr("case when gender = 'M' then 'Male' " +
      "when gender = 'F' then 'Female' " +
      "else 'Unknown' end"))

df3.show()

  val df4 = DF.select(col("*"),
    expr("case when gender = 'M' then 'Male' " +
      "when gender = 'F' then 'Female' " +
      "else 'Unknown' end").alias("new_gender123"))
df4.show()
import spark.implicits._
  val dataDF = Seq(
    (66, "a", "4"), (67, "a", "0"), (70, "b", "4"), (71, "d", "4"
    )).toDF("id", "code", "amt")
  dataDF.show()

  val df5 = dataDF.withColumn("new_data",when(col("code")==="a" || col("code")==="d","A")
    .when(col("code")==="b" && col("amt")===4,"B").otherwise("A1"))

  df5.show()
}
