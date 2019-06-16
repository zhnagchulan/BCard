package com.wanda.mob.spark.streaming.mysql

import java.sql.{Connection, DriverManager}

import com.wanda.mob.spark.streaming.event.impl.ScoreEvent
import com.wanda.mob.spark.streaming.main.Streaming.upsertMysqlURL
import com.wanda.mob.spark.streaming.utils.DateUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class MConnector {

  private def setProp(driver: String, username: String, password: String): java.util.Properties = {
    val properties = new java.util.Properties
    properties.setProperty("driver", driver)
    properties.setProperty("user", username)
    properties.setProperty("password", password)
    properties
  }

  def writeToMysqlByAppend(df: DataFrame,
                           table: String,
                           mysql_url: String,
                           driver: String,
                           username: String,
                           password: String): Unit = {
    df.write.mode(SaveMode.Append).jdbc(mysql_url, table, setProp(driver, username, password))
  }

  def upsertScoreEvent(rdd: RDD[ScoreEvent], upsertMysqlURL: String, tableName: String): Unit = {
    rdd.foreachPartition(iter => {

      val connection: Connection = DriverManager.getConnection(upsertMysqlURL)

      val sql = s"insert into $tableName values(?,?,?,?,?) ON DUPLICATE KEY UPDATE GROUPS=?,SCORE=?,LST_UPD_TIME=?"


      var parNum = 0
      val statement = connection.prepareStatement(sql)

      iter.foreach(e => {

        statement.setString(1, e.MEMBER_CODE)
        statement.setString(2, e.DATE_D)
        statement.setInt(3, e.GROUPS)
        statement.setString(4, e.SCORE)
        statement.setString(5, e.LST_UPD_TIME)
        statement.setInt(6, e.GROUPS)
        statement.setString(7, e.SCORE)
        statement.setString(8, e.LST_UPD_TIME)
        statement.addBatch()
        parNum += 1
        if (parNum >= 10000) {
          statement.executeBatch()
          parNum = 0
        }
      })
      statement.executeBatch()
    })
  }


  def upsertToTableDOrM(rdd: RDD[ScoreEvent],
                        upsertMysqlURL: String,
                        date: Broadcast[String],
                        mysql_url: String,
                        driver: String,
                        username: String,
                        password: String,
                        t1_table: String,
                        m_table: String): Unit = {

//    writeToMysqlByAppend(df,
//      t1_table,
//      mysql_url,
//      driver,
//      username,
//      password)

    upsertScoreEvent(
      rdd,
      upsertMysqlURL,
      t1_table
    )


    clearTwoDaysAgo(t1_table,
      mysql_url,
      driver,
      username,
      password,
      date)


    //如果是月初就刷新到历史表
    if (DateUtils.isFirstDayOfMonth(date.value)) {

      upsertScoreEvent(
        rdd,
        upsertMysqlURL,
        m_table
      )

    }
  }


  private def clearTwoDaysAgo(table: String,
                              mysql_url: String,
                              driver: String,
                              username: String,
                              password: String,
                              curDateBroadcast: Broadcast[String]): Unit = {

    val curDate = curDateBroadcast.value
    var connection: Connection = null
    try {
      //注册Driver
      Class.forName(driver)
      connection = DriverManager.getConnection(mysql_url, username, password)
      val statement = connection.createStatement
      val verifyData = statement.executeQuery(s"SELECT * from $table WHERE date_d = '${DateUtils.backdating(curDate, 1)}' limit 1")
      val bool = verifyData.next()
      println(bool, s"当前时间$curDate")
      if (bool) {
        statement.executeUpdate(s"delete from $table where date_d <= '${DateUtils.backdating(curDate, 2)}'")
        println("删除数据")
      } else {
        println("为空")
      }


    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }
}
