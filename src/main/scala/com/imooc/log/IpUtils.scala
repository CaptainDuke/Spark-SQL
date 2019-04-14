package com.imooc.log

import com.ggstar.util.ip.IpHelper

/*
 IP 解析工具类
 */
object IpUtils {

  def getCity(ip:String): Unit ={
    IpHelper.findRegionByIp("58.30.15.255")
  }

  def main(args: Array[String]): Unit = {
    println(getCity("58.30.15.255"))
  }


}
