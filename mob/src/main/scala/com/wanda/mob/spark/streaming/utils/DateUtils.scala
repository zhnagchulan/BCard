package com.wanda.mob.spark.streaming.utils

import java.text.SimpleDateFormat
import java.util.TimeZone


/**
  * Created by siyuan.tao.wb on 2017/11/1.
  */
object DateUtils {

  //  val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  //  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
  //  val DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd")

  /**
    * 计算时间差值（单位为天）
    * @param time1 时间1
    * @param time2 时间2
    * @return 差值
    */
  def minus(time1: String, time2: String): Int = {

    val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd")

    ((TIME_FORMAT.parse(time1).getTime - TIME_FORMAT.parse(time2).getTime)/(1000 * 60 * 60 * 24)).toInt

  }


  def isLessThanToday(time: String):Boolean={

    val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")

    DATE_FORMAT.format(TIME_FORMAT.parse(time).getTime) < DATE_FORMAT.format(currentCSTTimeZone())

  }

  def isLessOrEqualThanToday(time: String):Boolean={

    val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")

    DATE_FORMAT.format(TIME_FORMAT.parse(time).getTime) <= DATE_FORMAT.format(currentCSTTimeZone())

  }


  def today():String ={

    val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    TIME_FORMAT.format(currentCSTTimeZone())
  }

  /**
    * 精准到秒
    * @param time1
    * @param time2
    * @return
    */
  def after(time1: String, time2: String):Boolean={

    if(time1 == null || time2 == null) println(s"ERROR:before Fuc:time1= $time1 , time2 = $time2")

    val dateTime1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time1)
    val dateTime2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time2)
    if (dateTime1.after(dateTime2)) true else false
  }

  /**
    * 获取当前时间
    *
    * @return 毫秒时间戳
    */
  def getCurrentTime: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(currentCSTTimeZone())

//  /**
//    * 比较第一个参数是否是最新时间
//    *
//    * @param time1 第一个时间
//    * @param time2 第二个时间
//    * @return 判断结果
//    */
//  def getNewestOneByFormattedTime(time1: String, time2: String): String = {
//    try {
//      val dateTime1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time1)
//      val dateTime2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time2)
//      if (dateTime2.before(dateTime1)) return time1
//      else return time2
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//    print(Constants.ERROR_COMPARE_FLAG + ",time1=" + time1 + ",time2=" + time2)
//    Constants.ERROR_COMPARE_FLAG
//  }

  def getNewestOneByTimestamp(time1: String, time2: String): String = {
    if (time1.toLong - time2.toLong >= 0) {
      time1.toString
    } else {
      time2.toString
    }
  }


//  def timeFormat(time: Long): String = {
//    try {
//      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//      sdf.format(time)
//    }
//    catch {
//      case e: Exception =>
//        e.printStackTrace() + "------time:Long 为" + time
//        Constants.ERROR_TIME_FORMAT+time.toString
//    }
//  }

  /**
    * 上海时区
    * @return
    */
  private def currentCSTTimeZone(): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val date = sdf.format(System.currentTimeMillis)
    //必须用心的sdf 用上面tz过后的会无效
    val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf2.parse(date).getTime
  }


  def getLargestOneByTimeFormat(time1: String, time2: String): String = {
    try {

      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val dateTime1 = dateFormat.parse(time1)
      val dateTime2 = dateFormat.parse(time2)
      if (dateTime2.before(dateTime1)) return time1
      else return time2
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    print("ERROR:" + "time1=" + time1 + ",time2=" + time2)
    "ERROR"
  }

  def getLargestOneByTuple(t1: (String,String,Int), t2: (String,String,Int)): (String,String,Int) = {
    try {

      val time1 = t1._1
      val time2 = t2._1

      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val dateTime1 = dateFormat.parse(time1)
      val dateTime2 = dateFormat.parse(time2)
      if (dateTime2.before(dateTime1)) t1
      else t2
    } catch {
      case e: Exception =>
        print("ERROR:" + "t1=" + t1 + ",t2=" + t2)
        e.printStackTrace()
        ("ERROR","ERROR",0)
    }
  }

  def between(time: String, floor: String):Boolean={

    //after(head,time)
    after(time,floor) && isLessOrEqualThanToday(time)
  }

  def getCurrentMonth:String={
    val DATE_FORMAT = new SimpleDateFormat("yyyy-MM")

    DATE_FORMAT.format(currentCSTTimeZone())

  }

  def getCurrentDate:String={
    val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")

    DATE_FORMAT.format(currentCSTTimeZone())

  }

  def backdating(date:String,days: Int):String={

    val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")

    DATE_FORMAT.format(DATE_FORMAT.parse(date).getTime - (1000 * 60 * 60 * 24 * days))
  }


  def extractMonth(date:String):String={
    val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
    val MONTH_FORMAT = new SimpleDateFormat("yyyy-MM")
    MONTH_FORMAT.format(DATE_FORMAT.parse(date).getTime)
  }

  def isFirstDayOfMonth(date:String):Boolean = date.split("-")(2) == "01"
}
