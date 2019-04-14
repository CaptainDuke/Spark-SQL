package com.imooc.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/*
  第一步清洗：抽取出我们所需要的指定列的数据,运行在YARN 上
 */
object SparkStatFormatJobYARN {
  def main(args: Array[String]){

    if (args.length != 2){
      println("Usage: SparkStatFormatJobYARN <inputPath> <outputPath>")
      System.exit(1)
    }

    val Array(inputPath, outputPath) = args


    val spark = SparkSession.builder().getOrCreate()

    val accessRDD = spark.sparkContext.textFile(inputPath)
    accessRDD.take(10).foreach(println)

    val accessDF = spark.createDataFrame(accessRDD.map( x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

    accessDF.printSchema()

    accessDF.show(false)

    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
        .partitionBy("day")
        .save(outputPath)

    spark.stop()


  }
}
