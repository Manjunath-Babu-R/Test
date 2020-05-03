package SparkByExmple

import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object With_column_exmples extends App {
  val spark = SparkSession.builder()
    .appName("with")
    .master("local")
    .getOrCreate()
  val logger = LoggerFactory.getLogger(this.getClass.getName)
  Logger.getRootLogger.setLevel(Level.WARN)

  val data = Seq(Row(Row("James ","","Smith"),"36636","M","3000"),
    Row(Row("Michael ","Rose",""),"40288","M","4000"),
    Row(Row("Robert ","","Williams"),"42114","M","4000"),
    Row(Row("Maria ","Anne","Jones"),"39192","F","4000"),
    Row(Row("Jen","Mary","Brown"),"","F","-1")
  )
  val schema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("dob",StringType)
    .add("gender",StringType)
    .add("salary",StringType)
  val DF = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
      .withColumn("salary",col("salary").cast("Integer"))
      .withColumn("salary",col("salary")*100)
      .withColumn("salary_123",col("salary")* -1)
      .withColumn("county",lit("USA"))
      .withColumnRenamed("gender","sex")
      .drop("salary_123")
  DF.show()
  DF.printSchema()
  import spark.implicits._
  val columns1 = Seq("name","address")
  val data1 = Seq(("Robert, Smith", "1 Main st, Newark, NJ, 92537"),
    ("Maria, Garcia","3456 Walnut st, Newark, NJ, 94732"))
  var dfFromData1 = spark.createDataFrame(data1).toDF(columns1:_*)

  dfFromData1.show()
  dfFromData1.printSchema()
  import spark.implicits
  val newDF =dfFromData1.map(g => {
    val namesplit =g.getAs[String](0).split(",")
    val addsplit =g.getAs[String](1).split(",")
    (namesplit(0),namesplit(1),addsplit(0),addsplit(1),addsplit(2),addsplit(3))

  })

  val final_DF = newDF.toDF("First Name","Last Name","Address Line1","City","State","zipCode")
final_DF.show()
  final_DF.printSchema()

}
