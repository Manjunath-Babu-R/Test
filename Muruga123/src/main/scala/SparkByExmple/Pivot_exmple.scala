package SparkByExmple

import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

object Pivot_exmple extends App {
  val spark = SparkSession.builder()
    .appName("pivot")
    .master("local")
    .getOrCreate()
  val log = LoggerFactory.getLogger(this.getClass.getName)
  Logger.getRootLogger.setLevel(Level.WARN)

  val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
    ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
    ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

  import spark.implicits._

  val df = data.toDF("Product","Amount","Country")
  df.show()
val pivot1 = df.groupBy("product").pivot("Country").sum("Amount")
  pivot1.show()
  val countries = Seq("USA","China","Canada","Mexico")
  val pivot2 = df.groupBy("product").pivot("Country",countries).sum("Amount")
  pivot2.show()
import org.apache.spark.sql.functions._
  val unPivotDF = pivot2.select($"Product",
    expr("stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Amount)"))
    .where("Amount is not null")

  unPivotDF.show()
  val arrayStructureData = Seq(
    Row(Row("James ","","Smith"),List("Cricket","Movies"),Map("hair"->"black","eye"->"brown")),
    Row(Row("Michael ","Rose",""),List("Tennis"),Map("hair"->"brown","eye"->"black")),
    Row(Row("Robert ","","Williams"),List("Cooking","Football"),Map("hair"->"red","eye"->"gray")),
    Row(Row("Maria ","Anne","Jones"),null,Map("hair"->"blond","eye"->"red")),
    Row(Row("Jen","Mary","Brown"),List("Blogging"),Map("white"->"black","eye"->"black"))
  )
  val arrayStructureSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("hobbies", ArrayType(StringType))
    .add("properties", MapType(StringType,StringType))

  val DF8 = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)

  DF8.show()
  DF8.printSchema()

  case class Name(first:String,Last:String,middle:String)
  case class Employee(fullname:Name,age:Integer,gender:String)

  import org.apache.spark.sql.catalyst.ScalaReflection
  val schema =ScalaReflection.schemaFor[Employee].dataType.asInstanceOf[StructType]
  val end =Encoders.product[Employee].schema
  println(end)
  end.printTreeString()
}
