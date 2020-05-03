package SparkByExmple

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.storage.StorageLevel

object CreateRDD extends App {
  val spark = SparkSession.builder()
    .appName("create RDD")
    .master("local")
    .getOrCreate()

val logger = LoggerFactory.getLogger(this.getClass.getName)
  Logger.getRootLogger.setLevel(Level.WARN)
/*
  val rdd = spark.sparkContext.parallelize(Seq(("scala",100000),("spark",200000),("jave",40000)))
  rdd.foreach(println(_))
  val rdd1 = spark.sparkContext.parallelize(Seq.empty[String])
  rdd1.foreach(println(_))
  println(rdd1.getNumPartitions)
val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))
  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))
  //aggregate
  def p1 =(a:Int, v:(String,Int)) => a+v._2
  def p2 = (f:Int, h:Int) => f+h
  val agg = inputRDD.aggregate(0)(p1,p2)
  println(agg)
  ///treeAggregate
  def p3 =(a:Int, v:Int) => a+v
  def p4 = (f:Int, h:Int) => f+h
  val treeagg = listRdd.treeAggregate(0)(p3,p4)
  println(treeagg)
  //fold
  val flot1 = listRdd.fold(0)((a,b)=>a+b)
  println(flot1)
val float2 = inputRDD.fold(("total",0)){(a:(String,Int),b:(String,Int))=>
  val sum= a._2 + b._2
  ("total",sum)}
  println(float2)
  //reduce
  val reduce1= listRdd.reduce((a,b)=> a+b)
  println(reduce1)
  val reduce2 = inputRDD.reduce((a,b)=> ("Total233",a._2 + b._2))
  println(reduce2)
  //treeReduce
  val treere123 = listRdd.treeReduce((a,b)=>a+b)
  val tree333 = inputRDD.treeReduce((a,d)=>("Total33",a._2+ d._2))
  println(treere123)

  println("countbyvalue:"+listRdd.countByValue())
  println("Frist value :"+listRdd.first())
  println("Frist value333:"+inputRDD.first())
  //top

  println(" Top ::: "+listRdd.top(3).mkString(","))
  println(" Top33 ::::"+ inputRDD.top(2).mkString(","))
  //min

  println(" min :::"+listRdd.min())
  //take
  println(" Take ::::"+ listRdd.take(5).mkString(","))
  println("Take order::::"+ listRdd.takeOrdered(5).mkString(","))
  //println("Take sample :::: "+ listRdd.takeSample())*/
  val simpleData = Seq(("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  )
import spark.implicits._
  val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
  df.show()
  val df2 = df.groupBy("state").count()
  df2.show()
  println(df2.rdd.getNumPartitions)
  import org.apache.spark.storage.StorageLevel._
  val cache1 = df2.cache()
  val parsist112 = df2.persist(StorageLevel.DISK_ONLY)
  cache1.show()
  parsist112.show(false)
}
