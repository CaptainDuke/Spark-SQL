package com.imooc.log.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

object MysqlUtils {

  def getConnection() = {
//    DriverManager.getConnection("jdbc:mysql://10.42.2.22:3306/logbase?user=wang&password=wang&useSSL=false&useUnicode=true&characterEncoding=utf-8")
    DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/sql?user=root&password=&useSSL=false&useUnicode=true&characterEncoding=utf-8")

  }

  def release(connection: Connection, pstmt: PreparedStatement): Unit ={
    try {
      if(pstmt!=null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if(connection!=null) {
        connection.close()
      }
    }
  }
}
