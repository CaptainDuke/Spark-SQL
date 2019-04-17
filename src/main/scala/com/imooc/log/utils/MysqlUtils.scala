package com.imooc.log.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

object MysqlUtils {

  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:6603/database?user=root&password=Wyd_231231")
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
