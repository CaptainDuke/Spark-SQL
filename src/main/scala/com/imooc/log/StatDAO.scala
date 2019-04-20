package com.imooc.log

import java.sql.{Connection, PreparedStatement}

import com.imooc.log.utils.MysqlUtils

import scala.collection.mutable.ListBuffer

object StatDAO {


  def insertMinuteCity(list: ListBuffer[MinuteCityElement]): Unit ={
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into minute_city(minute, city, visit_times) values (?, ?, ?)"
      pstmt = connection.prepareStatement(sql)


      for(item <- list) {

        pstmt.setLong(1, item.minute)
        pstmt.setString(2, item.city)
        pstmt.setLong(3, item.times)


        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection, pstmt)
    }
  }


  def insertLabelMinuteTimes(list: ListBuffer[LabelMinuteElement]): Unit ={
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into label_minute(label, minute, visit_times) values (?, ?, ?)"
      pstmt = connection.prepareStatement(sql)

      for(item <- list) {
        pstmt.setString(1, item.label)
        pstmt.setLong(2, item.minute)
        pstmt.setLong(3, item.times)

        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection, pstmt)
    }
  }


  def insertLabelCityTimes(list: ListBuffer[LabelCityElement]): Unit ={
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into label_city(label, city, visit_times) values (?, ?, ?)"
      pstmt = connection.prepareStatement(sql)

      for(item <- list) {
        pstmt.setString(1, item.label)
        pstmt.setString(2, item.city)
        pstmt.setLong(3, item.times)

        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection, pstmt)
    }
  }
}
