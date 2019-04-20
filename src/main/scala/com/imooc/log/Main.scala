package com.imooc.log

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.imooc.log.utils._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}

import scala.collection.mutable.ListBuffer

object Main {

  def main(args: Array[String]): Unit = {



    val remote_flag = 0
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






    val spark = SparkSession.builder().appName("main").master("local[2]").getOrCreate()
//    val spark = SparkSession.builder().getOrCreate()



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
    val frameTemp = cleanedLogDF.joinWith(learnNVideoDF, learnNVideoDF("video.videoUrl")===cleanedLogDF("url")).toDF()
      .withColumnRenamed("_1", "originLog").
      withColumnRenamed("_2", "courseMenu")


    frameTemp.printSchema()
    frameTemp.show(false)
//    frameTemp.cache()
////    frameTemp.checkpoint()
//
//    cleanedLogDF.cache()
////    cleanedLogDF.checkpoint()


    // 优化：并行化入库
//    val arg = List[(SparkSession,DataFrame,DataFrame)=>Unit](labelCityTimes, labelMinuteTimes,minuteCityTimes)
//
//    arg.par.foreach(f=>f(spark,cleanedLogDF, frameTemp))
//
//    frameTemp.unpersist(true)
//    cleanedLogDF.unpersist(true)
//




//    非并行化入库
//    labelCityTimes(spark,cleanedLogDF, frameTemp)
//    labelMinuteTimes(spark,cleanedLogDF, frameTemp)
//
//    minuteCityTimes(spark,cleanedLogDF,frameTemp)
    spark.stop()
  }



  def minuteCityTimes(session: SparkSession, frame: DataFrame, learnNVideoDF: DataFrame): Unit ={
    import session.implicits._


    // 优化：将join表的操作统一提取到函数之外，将join之后的DataFrame传参进来
//    val frameTemp = frame.joinWith(learnNVideoDF, learnNVideoDF("video.videoUrl")===frame("url")).toDF()
//      .withColumnRenamed("_1", "originLog").withColumnRenamed("_2", "courseMenu")


    val minuteCity = frame.groupBy($"city", $"minute").agg(count("*").as("times"))





    //TODO: 广播变量的实现，可以优化入库效率
//    var list = new ListBuffer[MinuteCityElement] // new

    try {
      minuteCity.foreachPartition(partition => {
        val list = new ListBuffer[MinuteCityElement]  //old

        partition.foreach(record => {
          val minute = record.getAs[Long]("minute")
          val city = record.getAs[String]("city")
          val times = record.getAs[Long]("times")

          list.append(MinuteCityElement(minute, city, times))
//          StatDAO.insertMinuteCity(list)
        })

        StatDAO.insertMinuteCity(list)
      })

//      StatDAO.insertMinuteCity(list)
    } catch {
      case e: Exception => e.printStackTrace()
    }



  }



  def labelCityTimes(session: SparkSession, frame: DataFrame, frameTemp: DataFrame): Unit ={
    import session.implicits._

//    val frameTemp = frame.joinWith(learnNVideoDF, learnNVideoDF("video.videoUrl")===frame("url")).toDF()
//      .withColumnRenamed("_1", "originLog").withColumnRenamed("_2", "courseMenu")

    val labelCity = frameTemp.filter($"originLog.sourceType"==="video" || $"originLog.sourceType" === "code")
      .groupBy($"originLog.city", $"courseMenu.learn.label").agg(count("*").as("times"))

    try {
//      val list = new ListBuffer[LabelCityElement]
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
//      StatDAO.insertLabelCityTimes(list)
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }


  def labelMinuteTimes(session: SparkSession, frame: DataFrame, frameTemp: DataFrame): Unit = {
    import session.implicits._

    /*val frameTemp = frame.joinWith(learnNVideoDF, learnNVideoDF("video.videoUrl")===frame("url")).toDF()
      .withColumnRenamed("_1", "originLog").withColumnRenamed("_2", "courseMenu")
    */
    val labelMinute = frameTemp.filter($"originLog.sourceType" === "video" || $"originLog.sourceType" === "code")
      .groupBy($"originLog.minute", $"courseMenu.learn.label").agg(count("*").as("times"))

    try {
      //      val list = new ListBuffer[LabelMinuteElement]
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
      //      StatDAO.insertLabelMinuteTimes(list)

    } catch {
      case e: Exception => e.printStackTrace()
    }
  }}

