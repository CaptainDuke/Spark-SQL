package com.imooc.log

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.imooc.log.utils._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}

import scala.collection.mutable.ListBuffer

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StatInfo").master("local[2]").getOrCreate()

    val access = spark.sparkContext.textFile("file:///home/arby/Desktop/CloudProject/temp.log")

    val cleanedLogRDD = access.map(eachLine => {
      val splitsElements = eachLine.split(" ")
      val ip = splitsElements(0)
      val time = splitsElements(3) + " " + splitsElements(4)
      val url = splitsElements(11).replaceAll("\"", "")
      val traffic = splitsElements(9)

      //(ip, DateUtils.parseFormat(time), url, traffic)
      if(traffic!="0"
        && (url.contains("imooc.com/learn/")
        ||url.contains("imooc.com/video/")
        ||url.contains("imooc.com/article/")
        ||url.contains("imooc.com/code/")))
        DateUtils.parseFormat(time) + '\t' + url + '\t' + traffic + '\t' + ip
      else None
    }).filter(line => line!=None)

    val learnNameRDD = spark.sparkContext.textFile("file:///home/arby/imooc_learn_name_result.txt")
    val learnNameDF = spark.createDataFrame(learnNameRDD.map(eachLine => ConvertUtils.learnParser(eachLine)), ConvertUtils.learnStruct)

    val videoNameRDD = spark.sparkContext.textFile("file:///home/arby/imooc_video_name_result.txt")
    val videoNameDF = spark.createDataFrame(videoNameRDD.map(eachLine => ConvertUtils.videoParser(eachLine)), ConvertUtils.videoStruct)

    import spark.implicits._

    val learnNVideoDF = learnNameDF.joinWith(videoNameDF, learnNameDF("url")===videoNameDF("learnUrl"))

    learnNVideoDF.printSchema()
    learnNVideoDF.show(false)

    val cleanedLogDF = spark.createDataFrame(cleanedLogRDD.map(eachLine => ConvertUtils.parser(eachLine.toString)), ConvertUtils.struct)

    videoTopNPerDay(spark, cleanedLogDF)

    articleTopNPerDay(spark, cleanedLogDF)

    videoTopNPerDayPerCity(spark, cleanedLogDF)

    videoTopNPerDayTraffic(spark, cleanedLogDF)


    spark.stop()
  }



  def videoTopNPerDay(session: SparkSession, frame: DataFrame): Unit = {
    import session.implicits._

    val videoTopNDF = frame.filter($"sourceType"==="video" || $"sourceType" === "code")
      .groupBy($"day", $"url").agg(count("sourceId").as("times")).orderBy($"times".desc)

    //videoTopNDF.show(false)


    try {
      videoTopNDF.foreachPartition(partition => {
        val list = new ListBuffer[DayVideoTimes]

        partition.foreach(record => {
          val day = record.getAs[String]("day")
          val url = record.getAs[String]("url")
          val times = record.getAs[Long]("times")

          list.append(DayVideoTimes(day, url, times))
        })

        StatDAO.insertDayVideoTimes(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  def articleTopNPerDay(session: SparkSession, frame: DataFrame): Unit = {
    import session.implicits._

    val articleTopNDF = frame.filter($"sourceType"==="article")
      .groupBy($"day", $"url").agg(count("sourceId").as("times")).orderBy($"times".desc)

    //videoTopNDF.show(false)


    try {
      articleTopNDF.foreachPartition(partition => {
        val list = new ListBuffer[DayArticleTimes]

        partition.foreach(record => {
          val day = record.getAs[String]("day")
          val url = record.getAs[String]("url")
          val times = record.getAs[Long]("times")

          list.append(DayArticleTimes(day, url, times))
        })

        StatDAO.insertDayArticleTimes(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def videoTopNPerDayPerCity(session: SparkSession, frame: DataFrame): Unit ={
    import session.implicits._

    val videoTopNDF = frame.filter($"sourceType"==="video")
      .groupBy($"day",$"city", $"url").agg(count("url").as("times"))

    val resultDF = videoTopNDF.select(
      videoTopNDF("day"),
      videoTopNDF("city"),
      videoTopNDF("url"),
      videoTopNDF("times"),
      row_number().over(Window.partitionBy(videoTopNDF("city"))
        .orderBy(videoTopNDF("times").desc)
      ).as("times_rank")
    ).filter("times_rank <= 3")

    try {
      resultDF.foreachPartition(partition => {
        val list = new ListBuffer[DayVideoTimesCity]

        partition.foreach(record => {
          val day = record.getAs[String]("day")
          val url = record.getAs[String]("url")
          val times = record.getAs[Long]("times")
          val city = record.getAs[String]("city")
          val timesRank = record.getAs[Long]("times_rank")


          list.append(DayVideoTimesCity(day, url, times, city, timesRank))
        })

        StatDAO.insertDayVideoTimesCity(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def articleTopNPerDayPerCity(session: SparkSession, frame: DataFrame): Unit ={
    import session.implicits._

    val articleTopNDF = frame.filter($"sourceType"==="article")
      .groupBy($"day",$"city", $"url").agg(count("url").as("times"))

    val resultDF = articleTopNDF.select(
      articleTopNDF("day"),
      articleTopNDF("city"),
      articleTopNDF("url"),
      articleTopNDF("times"),
      row_number().over(Window.partitionBy(articleTopNDF("city"))
        .orderBy(articleTopNDF("times").desc)
      ).as("times_rank")
    ).filter("times_rank <= 3")

    try {
      resultDF.foreachPartition(partition => {
        val list = new ListBuffer[DayArticleTimesCity]

        partition.foreach(record => {
          val day = record.getAs[String]("day")
          val url = record.getAs[String]("url")
          val times = record.getAs[Long]("times")
          val city = record.getAs[String]("city")
          val timesRank = record.getAs[Long]("times_rank")


          list.append(DayArticleTimesCity(day, url, times, city, timesRank))
        })

        StatDAO.insertDayArticleTimesCity(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def videoTopNPerDayTraffic(session: SparkSession, frame: DataFrame): Unit = {
    import session.implicits._

    val videoTopNDF = frame.filter($"sourceType"==="video")
      .groupBy($"day", $"url").agg(count("traffic").as("traffics"))
      .orderBy($"traffics".desc)

    try {
      videoTopNDF.foreachPartition(partition => {
        val list = new ListBuffer[DayVideoTraffics]

        partition.foreach(record => {
          val day = record.getAs[String]("day")
          val url = record.getAs[String]("url")
          val traffics = record.getAs[Long]("traffics")

          list.append(DayVideoTraffics(day, url, traffics))
        })

        StatDAO.insertDayVideoTraffics(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def articleTopNPerDayTraffic(session: SparkSession, frame: DataFrame): Unit = {
    import session.implicits._

    val articleTopNDF = frame.filter($"sourceType"==="article")
      .groupBy($"day", $"url").agg(count("traffic").as("traffics"))
      .orderBy($"traffics".desc)

    try {
      articleTopNDF.foreachPartition(partition => {
        val list = new ListBuffer[DayArticleTraffics]

        partition.foreach(record => {
          val day = record.getAs[String]("day")
          val url = record.getAs[String]("url")
          val traffics = record.getAs[Long]("traffics")

          list.append(DayArticleTraffics(day, url, traffics))
        })

        StatDAO.insertDayArticleTraffics(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
