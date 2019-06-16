package com.wanda.mob.spark.streaming.utils

import java.lang.Double

import com.wanda.mob.spark.streaming.event.impl.CommonEvent
import com.wanda.mob.spark.streaming.models.{Model_Three, Model_Two}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
//import com.wanda.mob.spark.streaming.main.Streaming.session
import com.wanda.mob.spark.streaming.main.Offline.session

object BCardModel {

  def modelThree(ss: RDD[Tuple2[String, Iterable[CommonEvent]]]) = {
//    val kequn3 = getTable(ss,session).mapPartitions(item =>
  val kequn3 = getTable(ss).mapPartitions(item =>
    for (x <- item) yield {
        if (x._2 <= 0.01 && x._3 > 0 && x._4 > 0 && x._5 > 0.01) x._1 else null
      }
    ).filter(x => x != null)
      .mapPartitions(item =>
        for (x <- item) yield {
          (x, 1)
        }
      ).groupByKey().join(ss).mapPartitions(item => {
      for (x <- item) yield {
        (x._1, x._2._2)
      }
    })
    Model_Three.scoring("客群3",kequn3).mapPartitions(item => {
      for (x <- item) yield {
        (x._1,x._2.toString, "3", x._3)
      }
    })
  }

  def modelTwo(ss: RDD[Tuple2[String, Iterable[CommonEvent]]]) = {
//    val kequn2 = getTable(ss,session).mapPartitions(item => {
      val kequn2 = getTable(ss).mapPartitions(item => {

        for (x <- item) yield {
        if (x._2 <= 0.01 && x._3 > 0 && x._4 <= 0 && x._5 > 0.01) x._1 else null
      }
    }).filter(x => x != null).
      mapPartitions(item => {
        for (x <- item) yield {
          (x, 1)
        }
      }
      ).groupByKey().join(ss)
      .mapPartitions(item => {
        for (x <- item) yield {
          (x._1, x._2._2)
        }
      })
   // kequn2.map(x=>x._2.foreach(x=>println(x.CUST_NBR,x.LST_UPD_TIME,x.TRAN_AMT_PAID))
    Model_Two.scoring("客群2",kequn2).mapPartitions(item => {
      for (x <- item) yield {
        (x._1, x._2.toString, "2", x._3)
      }
    })

  }

  def noScore(ss: RDD[Tuple2[String, Iterable[CommonEvent]]]) = {
//    val noScoreKequn = getTable(ss,session).mapPartitions(item => {
      val noScoreKequn = getTable(ss).mapPartitions(item => {

        for (x <- item) yield {
        if (x._2 <= 0 && x._3 <= 0) {
          (x._1,"NULL", "1", x._6)
        } else if (x._2 <= 0 && x._3 > 0 && x._4 <= 0 && x._5 <= 0.01) {
          (x._1,"NULL", "2", x._6)
        } else if (x._2 <= 0 && x._3 > 0 && x._4 > 0 && x._5 <= 0.01) {
          (x._1, "NULL", "3", x._6)
        } else {
          null
        }
      }
    }).filter(x => x != null)
//    noScoreKequn.foreach(println)
    noScoreKequn
  }

  /** * 当前余额Balance
    * 近一年还款OneYear
    * 历史最大逾期天数MaxODay
    * 近三个月余额 ThreeBalance
    *
    */
  def getTable(ss: RDD[Tuple2[String, Iterable[CommonEvent]]]) = {
//def getTable(ss: RDD[Tuple2[String, Iterable[CommonEvent]]],session:SparkSession) = {
    import session.implicits._

    val rdd = ss.flatMap(x => {
      for (i <- x._2) yield {
        i
      }
    }).mapPartitions(x =>
      for (i <- x) yield {
        (i.CUST_NBR, i.ACCOUNT_NMBR, i.PYMT_FLAG, i.DELQ_STATUS, Double.valueOf(i.PRINCIPAL), Double.valueOf(i.TRAN_AMT_PAID),
          i.CASH_AMT, i.POSTING_DTE, i.STATUS, i.TRANSACTION_TYPE, i.PAYMENT_DTE,
          i.LST_UPD_TIME, i.AUDIT_TIME)
      })
      .toDF("CUST_NBR", "ACCOUNT_NMBR", "PYMT_FLAG", "DELQ_STATUS", "PRINCIPAL", "TRAN_AMT_PAID",
        "CASH_AMT", "POSTING_DTE", "STATUS", "TRANSACTION_TYPE", "PAYMENT_DTE", "LST_UPD_TIME", "AUDIT_TIME").repartition(1)
//    rdd.printSchema()
    rdd.createOrReplaceTempView("wandaNow")
    val result = session.sql(
      """
        |select b.cust_nbr,sum(b.principal)-sum(b.tran_amt_paid) as balance,
        |SUM(CASE WHEN DATEDIFF(NOW(), POSTING_DTE)<=365 THEN TRAN_AMT_PAID ELSE 0 END) AS OneYear,
        |MAX(b.oday) AS MaxOday,
        |SUM(b.threeBalance) AS threeBalance,
        |MAX(b.maxLst_upd_time)AS max_lst_upd_time
        |from(
        |select cust_nbr,account_nmbr,principal,POSTING_DTE,sum(tran_amt_paid) as tran_amt_paid,
        |SUM(CASE WHEN DATEDIFF(NOW(), POSTING_DTE)<=365 THEN TRAN_AMT_PAID ELSE 0 END) AS OneYear,
        |MAX((CASE WHEN delq_status='1' AND pymt_flag='1' OR pymt_flag='01' THEN DATEDIFF(LST_UPD_TIME,PAYMENT_DTE)
        |WHEN delq_status='1' AND pymt_flag='0' OR pymt_flag='00' THEN DATEDIFF(NOW(),PAYMENT_DTE) ELSE 0 END))AS oday,
        |SUM(CASE WHEN DATEDIFF(NOW(),LST_UPD_TIME)<=90 THEN TRAN_AMT_PAID ELSE 0 END) AS threeBalance,
        |MAX(LST_UPD_TIME)AS maxLst_upd_time
        |from wandaNow
        |group by 1,2,3,4)b
        |group by 1
      """.stripMargin)
//    result.show(1000)
    val kequn = result.rdd.map(x => (x.get(0).toString, x.get(1).formatted("%.5f").toDouble,
      x.get(2).formatted("%.5f").toDouble, x.get(3).toString.toDouble, x.get(4).formatted("%.5f").toDouble, x.get(5).toString))
    kequn
  }
}