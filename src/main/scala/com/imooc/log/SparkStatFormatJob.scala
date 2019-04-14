package com.imooc.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/*
  第一步清洗：抽取出我们所需要的指定列的数据
 */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob")
      .master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file:///Users/captwang/workspace/imooc/spark-sql/data/access10000.log")
    accessRDD.take(10).foreach(println)

    val accessDF = spark.createDataFrame(accessRDD.map( x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

    accessDF.printSchema()

    accessDF.show(false)

    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
        .partitionBy("day")
        .save("/Users/captwang/workspace/imooc/spark-sql/clean")

    spark.stop()


  }
}
