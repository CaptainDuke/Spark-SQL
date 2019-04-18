package com.imooc.log

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.imooc.log.utils._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}

import scala.collection.mutable.ListBuffer

object Main {

  def main(args: Array[String]): Unit = {



    val remote_flag = 1
    /*
      1. 改 flag
      2. 改 master("local[2]")
      3. 改 pom.xml
      4. 改 MysqlUtils -uwang -pwang
     */



    //val Array(inputPath, learnFile, videoFile, outputPath) = ("", "", "", "")

    var inputPath = ""
    var learnFile = ""
    var videoFile = ""
    var outputPath = ""
    if (remote_flag==1){
      //val Array(inputPath, learnFile, videoFile, outputPath) = args
      if(args.length != 4){

        println("<input log path>", "<input learn file>", "<input video file>", "<output files>")
        System.exit(1)
      }
      inputPath = args(0)
      learnFile = args(1)
      videoFile = args(2)
      outputPath = args(3)

    } else{
      inputPath = "file:///Users/captwang/Desktop/temp.log"
      learnFile = "file:///Users/captwang/Desktop/imooc_learn_name_result2.txt"
      videoFile = "file:///Users/captwang/Desktop/imooc_video_name_result.txt"
      //spark = SparkSession.builder().appName("main").master("local[2]").getOrCreate()
    }

    //






//    val spark = SparkSession.builder().appName("main").master("local[2]").getOrCreate()
    val spark = SparkSession.builder().getOrCreate()



    val access = spark.sparkContext.textFile(inputPath) //"file:///Users/captwang/Desktop/temp.log")

    val learnNameRDD = spark.sparkContext.textFile(learnFile)//"file:///Users/captwang/Desktop/imooc_learn_name_result.txt")

    val videoNameRDD = spark.sparkContext.textFile(videoFile)//"file:///Users/captwang/Desktop/imooc_video_name_result.txt")


    val cleanedLogRDD = access.map(eachLine => {
      val splitsElements = eachLine.split(" ")
      val ip = splitsElements(0)
      val time = splitsElements(3) + " " + splitsElements(4)
      val url = splitsElements(11).replaceAll("\"", "")
      val traffic = splitsElements(9)

      //(ip, DateUtils.parseFormat(time), url, traffic)(url.contains("imooc.com/learn/")
      if(traffic!="0"

        &&(url.contains("imooc.com/video/")
        ||url.contains("imooc.com/article/")
        ||url.contains("imooc.com/code/")
        ||url.contains("imooc.com/learn/")))
        DateUtils.parseFormat(time) + '\t' + url + '\t' + traffic + '\t' + ip
      else None
    }).filter(line => line!=None)


    // 为了查询脏数据
    /*val temp = cleanedLogRDD
    temp.repartition(1).saveAsTextFile(outputPath)//"file:///Users/rocky/data/imooc/output/")*/

    val learnNameDF = spark.createDataFrame(learnNameRDD.map(eachLine => ConvertUtils.learnParser(eachLine)), ConvertUtils.learnStruct)

    val videoNameDF = spark.createDataFrame(videoNameRDD.map(eachLine => ConvertUtils.videoParser(eachLine)), ConvertUtils.videoStruct)

    import spark.implicits._

    val learnNVideoDF = learnNameDF.joinWith(videoNameDF, learnNameDF("url")===videoNameDF("url")).toDF()
      .withColumnRenamed("_1", "learn").withColumnRenamed("_2", "video")

//    learnNVideoDF.printSchema()
//    learnNVideoDF.show(false)

    val cleanedLogDF = spark.createDataFrame(cleanedLogRDD.map(eachLine => ConvertUtils.parser(eachLine.toString)), ConvertUtils.struct)
//    cleanedLogDF.printSchema()
//    cleanedLogDF.show(false)

//    videoTopNPerMinute(spark, cleanedLogDF, learnNVideoDF)

//
//    articleTopNPerDay(spark, cleanedLogDF)
//
//    videoTopNPerDayPerCity(spark, cleanedLogDF)
//
//    videoTopNPerDayTraffic(spark, cleanedLogDF)
//

    val arg = List[(SparkSession,DataFrame,DataFrame)=>Unit](labelCityTimes, labelMinuteTimes,minuteCityTimes)

    arg.par.foreach(f=>f(spark,cleanedLogDF, learnNVideoDF))
    /*labelCityTimes(spark,cleanedLogDF, learnNVideoDF)
    labelMinuteTimes(spark,cleanedLogDF, learnNVideoDF)

    minuteCityTimes(spark,cleanedLogDF,learnNVideoDF)*/
    spark.stop()
  }



  def minuteCityTimes(session: SparkSession, frame: DataFrame, learnNVideoDF: DataFrame): Unit ={
    import session.implicits._

//    val frameTemp = frame.joinWith(learnNVideoDF, learnNVideoDF("video.videoUrl")===frame("url")).toDF()
//      .withColumnRenamed("_1", "originLog").withColumnRenamed("_2", "courseMenu")
//
//    frameTemp.printSchema()
//    frameTemp.show(false)

    val minuteCity = frame.groupBy($"city", $"minute").agg(count("*").as("times"))

    //labelCity.show(false)



    try {
      minuteCity.foreachPartition(partition => {
        val list = new ListBuffer[MinuteCityElement]

        partition.foreach(record => {
          val minute = record.getAs[Long]("minute")
          val city = record.getAs[String]("city")
          val times = record.getAs[Long]("times")

          list.append(MinuteCityElement(minute, city, times))
        })

        StatDAO.insertMinuteCity(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }



  def labelCityTimes(session: SparkSession, frame: DataFrame, learnNVideoDF: DataFrame): Unit ={
    import session.implicits._

    val frameTemp = frame.joinWith(learnNVideoDF, learnNVideoDF("video.videoUrl")===frame("url")).toDF()
      .withColumnRenamed("_1", "originLog").withColumnRenamed("_2", "courseMenu")

//    frameTemp.printSchema()
//    frameTemp.show(false)
    val labelCity = frameTemp.filter($"originLog.sourceType"==="video" || $"originLog.sourceType" === "code")
      .groupBy($"originLog.city", $"courseMenu.learn.label").agg(count("*").as("times"))

//    labelCity.show(false)



    try {
      labelCity.foreachPartition(partition => {
        val list = new ListBuffer[LabelCityElement]

        partition.foreach(record => {
          val label = record.getAs[String]("label")
          val city = record.getAs[String]("city")
          val times = record.getAs[Long]("times")

          list.append(LabelCityElement(label, city, times))
        })

        StatDAO.insertLabelCityTimes(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }


  def labelMinuteTimes(session: SparkSession, frame: DataFrame, learnNVideoDF: DataFrame): Unit ={
    import session.implicits._

    val frameTemp = frame.joinWith(learnNVideoDF, learnNVideoDF("video.videoUrl")===frame("url")).toDF()
      .withColumnRenamed("_1", "originLog").withColumnRenamed("_2", "courseMenu")

//    frameTemp.printSchema()
//    frameTemp.show(false)
    val labelMinute = frameTemp.filter($"originLog.sourceType"==="video" || $"originLog.sourceType" === "code")
      .groupBy($"originLog.minute", $"courseMenu.learn.label").agg(count("*").as("times"))

//    labelMinute.show(false)



    try {
      labelMinute.foreachPartition(partition => {
        val list = new ListBuffer[LabelMinuteElement]

        partition.foreach(record => {
          val label = record.getAs[String]("label")
          val minute = record.getAs[Long]("minute")
          val times = record.getAs[Long]("times")

          list.append(LabelMinuteElement(label, minute, times))
        })

        StatDAO.insertLabelMinuteTimes(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }




  def videoTopNPerMinute(session: SparkSession, frame: DataFrame, learnNVideoDF: DataFrame): Unit = {
    import session.implicits._

/*    frame.printSchema()
    frame.show(false)
    learnNVideoDF.printSchema()
    learnNVideoDF.show(false)*/

//    learnNVideoDF.select($"video.videoUrl").show(false)


    val frameTemp = frame.joinWith(learnNVideoDF, learnNVideoDF("video.videoUrl")===frame("url")).toDF()
      .withColumnRenamed("_1", "originLog").withColumnRenamed("_2", "courseMenu")

//    frameTemp.printSchema()
//    frameTemp.show(false)
    val videoTopNDF = frameTemp.filter($"originLog.sourceType"==="video" || $"originLog.sourceType" === "code")
      .groupBy($"originLog.minute", $"courseMenu.learn.name").agg(count("*").as("times")).orderBy($"times".desc)

//    videoTopNDF.show(false)



    try {
      videoTopNDF.foreachPartition(partition => {
        val list = new ListBuffer[MinuteVideoTimes]

        partition.foreach(record => {
          val minute = record.getAs[Long]("minute")
          val name = record.getAs[String]("name")
          val times = record.getAs[Long]("times")

          list.append(MinuteVideoTimes(minute, name, times))
        })

        StatDAO.insertMinuteVideoTimes(list)
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
