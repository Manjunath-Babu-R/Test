package com

import org.apache.spark.sql.SparkSession

object RDD_collections extends  App {

  val spark = SparkSession.builder()
    .appName("RDD collecton")
    .master("local")
    .getOrCreate()
  val rdd1 = spark.sparkContext.parallelize(Seq(1,5,4,3,11,33,6,8))
  rdd1.glom().collect()

}
