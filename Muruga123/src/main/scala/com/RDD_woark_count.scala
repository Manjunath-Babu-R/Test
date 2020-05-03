package com

import org.apache.spark.sql.SparkSession

object RDD_woark_count extends  App{

  val spark = SparkSession.builder()
    .appName("word_count")
    .master("local")
    .getOrCreate()

  val readTestfile = spark.sparkContext.textFile("C:\\Users\\Lenovo\\Desktop\\words.txt")
  val fal = readTestfile.flatMap(t => t.split(" "))
  val keyvalues = fal.map(t=>(t,1))
  //val group = keyvalues.groupBy(a=>a)
  //val word_count = group.map(t => (t._1,t._2.size))
  val red = keyvalues.reduceByKey((a,b)=> a+b).sortBy(t => -t._1.size)
  val ggg = fal.mapPartitions(x => x.next().iterator)

ggg.collect().foreach(println(_))



}
