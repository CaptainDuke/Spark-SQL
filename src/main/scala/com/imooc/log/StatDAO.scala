package com.imooc.log

import java.sql.{Connection, PreparedStatement}

import com.imooc.log.utils.MysqlUtils

import scala.collection.mutable.ListBuffer

object StatDAO {
  def insertDayVideoTimes(list: ListBuffer[DayVideoTimes]): Unit ={
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into table_name(attr1, attr2, attr3) values (?, ?, ?)"
      pstmt = connection.prepareStatement(sql)

      for(item <- list) {
        pstmt.setString(1, item.day)
        pstmt.setString(2, item.url)
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

  def insertDayArticleTimes(list: ListBuffer[DayArticleTimes]): Unit ={
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into table_name(attr1, attr2, attr3) values (?, ?, ?)"
      pstmt = connection.prepareStatement(sql)

      for(item <- list) {
        pstmt.setString(1, item.day)
        pstmt.setString(2, item.url)
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

  def insertDayVideoTimesCity(list: ListBuffer[DayVideoTimesCity]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into table_name(attr1, attr2, attr3, attr4, attr5) values (?, ?, ?, ?, ?)"
      pstmt = connection.prepareStatement(sql)

      for(item <- list) {
        pstmt.setString(1, item.day)
        pstmt.setString(2, item.url)
        pstmt.setLong(3, item.times)
        pstmt.setString(2, item.city)
        pstmt.setLong(3, item.rank)

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

  def insertDayArticleTimesCity(list: ListBuffer[DayArticleTimesCity]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into table_name(attr1, attr2, attr3, attr4, attr5) values (?, ?, ?, ?, ?)"
      pstmt = connection.prepareStatement(sql)

      for(item <- list) {
        pstmt.setString(1, item.day)
        pstmt.setString(2, item.url)
        pstmt.setLong(3, item.times)
        pstmt.setString(2, item.city)
        pstmt.setLong(3, item.rank)

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

  def insertDayVideoTraffics(list: ListBuffer[DayVideoTraffics]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into table_name(attr1, attr2, attr3) values (?, ?, ?)"
      pstmt = connection.prepareStatement(sql)

      for(item <- list) {
        pstmt.setString(1, item.day)
        pstmt.setString(2, item.url)
        pstmt.setLong(3, item.traffics)

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

  def insertDayArticleTraffics(list: ListBuffer[DayArticleTraffics]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into table_name(attr1, attr2, attr3) values (?, ?, ?)"
      pstmt = connection.prepareStatement(sql)

      for(item <- list) {
        pstmt.setString(1, item.day)
        pstmt.setString(2, item.url)
        pstmt.setLong(3, item.traffics)

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