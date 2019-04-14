package com.imooc.log

import com.ggstar.util.ip.IpHelper

/*
 IP 解析工具类
 */
object IpUtils {

  def getCity(ip:String) ={
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]) {
    println(getCity("58.30.15.255"))
  }


}
