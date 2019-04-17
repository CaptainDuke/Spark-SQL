package com.imooc.log.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object ConvertUtils {

  val struct = StructType(
    Array(
      StructField("url", StringType),
      StructField("sourceType", StringType),
      StructField("sourceId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("minute", LongType)
    )
  )

  val learnStruct = StructType(
    Array(
      StructField("url", StringType),
      StructField("name", StringType)
    )
  )

  val videoStruct = StructType(
    Array(
      StructField("url", StringType),
      StructField("videoUrl", StringType),
      StructField("name", StringType)
    )
  )


  def parser(log: String) = {

    try {
      val splits = log.split("\t")

      val time = splits(0)
      val day = time.substring(11, 19).split(":")

      val minute = day(0).toLong * 60l + day(1).toLong



      var url = splits(1)
      val domain = "imooc.com"
      val source = url.substring(url.indexOf(domain)+domain.length)
      url = source
      val sourceTypeNId = source.split("/")
      var sourceType = ""
      var sourceId = 0l
      if(sourceTypeNId.length > 1) {
        sourceType = sourceTypeNId(1)
        sourceId = sourceTypeNId(2).toLong
      }

      val traffic = splits(2).toLong

      val ip = splits(3)
      var city = IpUtils.getCity(ip)

      Row(url, sourceType, sourceId, traffic, ip, city, time, minute)
    } catch {
      case e: Exception => Row("","",0l,0l,"","","",0l)
    }

  }

  def videoParser(record: String) = {

    try {
      val splits = record.split(",")

      var learnUrl = ""
      var url = ""
      var name = ""

      learnUrl = splits(0)
      url = splits(1).replaceAll(" ", "")
      name = splits(2)


      Row(learnUrl, url, name)
    } catch {
      case e: Exception => Row(0)
    }

  }


  def learnParser(record: String) = {

    try {
      val splits = record.split(",")

      var url = ""
      var name = ""

      url = splits(0)
      name = splits(1)


      Row(url, name)
    } catch {
      case e: Exception => {
        print(e.printStackTrace())
        Row(0)
      }

    }

  }

  def main(args: Array[String]): Unit = {
    print(learnParser("/learn/90, 展开与收起效果"))
    print(videoParser("/learn/40, /video/485, 5-1学以致用——Sublime综合技巧运用(07:00)"))
    val time = "2016-11-09 10:01:02"
    val day = time.substring(11, 19).split(":")

    val minute = day(0).toLong * 60l + day(1).toLong
    print(minute)
  }
}
