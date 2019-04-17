package com.imooc.log.utils

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {
  //val input_format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  val input_format = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  //val output_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val output_format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def parseFormat(time: String) = {
    output_format.format(new Date(longTypeTime(time)))
  }

  def longTypeTime(time: String) = {
    try {
      input_format.parse(time.substring(time.indexOf("[")+1, time.lastIndexOf("]"))).getTime
    } catch {
      case e: Exception => 0l
    }
  }

  def main(args: Array[String])= {
    println(parseFormat("[09/Nov/2016:00:01:02 +0800]"))
  }
}
