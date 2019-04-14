package com.imooc.log.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

object MysqlUtils {

  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/database?user=root&password=xxxx")
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
