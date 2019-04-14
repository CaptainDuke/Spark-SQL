package com.imooc.log
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
  统计TOP N 的 Spark 作业
 */
object TopNStatJob {

  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame): Unit = {

//    import spark.implicits._
//
//    val videoAccessTopNDF = accessDF.filter($"day" === "20170511" && $"cmsType" === "video")
//      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)
//
//    videoAccessTopNDF.show(false)

    accessDF.createOrReplaceTempView("access_logs")
    val videoAccessTopNDF = spark.sql("select day, cmsId, count(1) as times from access_logs " +
      "where day='20170511' and cmsType='video' " +
      "group by day, cmsId order by times desc")
    videoAccessTopNDF.show(false)


  }

  def main(args: Array[String]){
    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
      .master("local[2]").getOrCreate()

    val accessDF = spark.read.format("parquet").load("/Users/captwang/workspace/imooc/spark-sql/clean")
    accessDF.printSchema()
    accessDF.show(false)

    videoAccessTopNStat(spark, accessDF)

  }

}
