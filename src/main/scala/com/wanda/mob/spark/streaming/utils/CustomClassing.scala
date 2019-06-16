package com.wanda.mob.spark.streaming.utils

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.lang.Double

import com.wanda.mob.spark.streaming.event.impl.CommonEvent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.wanda.mob.spark.streaming.main.Streaming.sc
import com.wanda.mob.spark.streaming.main.New_Offline.sc

object CustomClassing {
  //****************************************************************************************************************
  /*1.if delq_status=1 and pymt_flag=0 then yq_flag=1 else yq_flag=0
  *sum(yq_flag) as yq_cnt by member_code
  *if yq_cnt>1 then 用户当前有逾期欠款;if yq_cnt<=0 then 用户当前无逾期欠款
**/

  def getYqCustomRdd(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[String] = {
    return baseRdd
      .mapPartitions(itertor => itertor.filter(t => (t._3=="00"||t._3=="0") && (t._4=="01"||t._4=="1")).map(t => (t._1, 1)))
      .reduceByKey(_ + _).filter(t => t._2 >= 1).mapPartitions(itertor => itertor.map(t => t._1)).distinct().cache()
  }

  //用baseRdd集合减去逾期的用户得到的是无逾期的客户
  def getNoYqCustomRdd(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[String] = {
    return baseRdd.map(t => t._1).distinct().subtract(getYqCustomRdd(baseRdd)).cache()
  }

  //*******************************************************************************************************************
  /*
  * 计算用户当前金额的使用情况（principal-tran_amt_paid)/cash_amt）
  *根据用户所有未还清的欠款还款记录，计算剩余金额使用率（不含利息)
  *
  *
  * 只要用户有一笔交易有余额（即该笔交易shiDongRate>0）就判定用户有余额
  * */

  def getBankBlanceCustomRdd(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[String] = {
    val BankBlanceCustomeRdd = baseRdd.mapPartitions(itertor => itertor.map(t => ((t._1, t._2, t._5, t._7), Double.valueOf(t._6))))
      .reduceByKey(_ + _).mapPartitions(itertor => itertor.map(t => {
      val PRINCIPAL = Double.valueOf(t._1._3)
      val CASH_AMT = Double.valueOf(t._1._4)
      val TRAN_AMT_PAID_total = t._2
      val leftMoney: Double = PRINCIPAL - TRAN_AMT_PAID_total
      (t._1._1, leftMoney)
    })).reduceByKey(_ + _).filter(t => t._2 > 0.01).map(t => t._1).distinct().intersection(getNoYqCustomRdd(baseRdd)).cache()
    return BankBlanceCustomeRdd
  }

  //无余额RDD=逾期RDD-有余额RDD
  def getNoBankBlanceCustomRdd(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[String] = {
    return getNoYqCustomRdd(baseRdd).subtract(getBankBlanceCustomRdd(baseRdd))
  }

  //****************************************************************************************************************
  /**  （1）if delq_status=1 and pymt_flag=1 then oday=datepart(lst_upd_time)-datepart(payment_dte); if delq_status=1 and pymt_flag=0 then oday=today-datepart(payment_dte)
     *  （2）max(oday) as oday_max group by member_code
     *  （3）if oday_max>0 用户历史有逾期， if oday_max<=0 用户历史无逾期
     *
     * */
  //历史逾期[1,3]天客户RDD
  def getMaxYqOneToThreeCustomRdd(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[String] = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val toDay = dateFormat.parse(dateFormat.format(new Date())).getTime
    val firstRdd = baseRdd.mapPartitions(
      itertor => itertor.filter(t => !t._3.contains("1") && t._4.contains("1")).
        map(t => (t._1, toDay - dateFormat.parse(t._11).getTime))
    ).cache()
    val secondRdd = baseRdd.mapPartitions(
      itertor => itertor.filter(t => t._3.contains("1") && t._4.contains("1")).
        map(t => (t._1, dateFormat.parse(t._12).getTime - (dateFormat.parse(t._11).getTime)))
    ).cache()
    return firstRdd.union(secondRdd).groupByKey().
    mapPartitions(itertor => itertor.map(t => (t._1, t._2.max)).filter(t => t._2 / (1000 * 60 * 60 * 24) <= 3 && t._2 / (1000 * 60 * 60 * 24) > 0)
      .map(t => t._1)).intersection(getBankBlanceCustomRdd(baseRdd)).cache()
}

  //历史逾期[4,+)的客户RDD
  def getMaxYqGreatFourCustomRdd(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[String] = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val toDay = dateFormat.parse(dateFormat.format(new Date())).getTime
    val firstRdd = baseRdd.mapPartitions(
      itertor => itertor.filter(t => !t._3.contains("1") && t._4.contains("1")).
        map(t => (t._1, toDay - (dateFormat.parse(t._11).getTime)))
    ).cache()
    val secondRdd = baseRdd.mapPartitions(
      itertor => itertor.filter(t => t._3.contains("1") && t._4.contains("1")).
        map(t => (t._1, dateFormat.parse(t._12).getTime - (dateFormat.parse(t._11).getTime)))
    ).cache()
    return firstRdd.union(secondRdd).groupByKey().
      mapPartitions(itertor => itertor.map(t => (t._1, t._2.max)).filter(t => t._2 / 1000 / 60 / 60 / 24 >= 4)
        .map(t => t._1)).intersection(getBankBlanceCustomRdd(baseRdd)).cache()
  }

  //历史无逾期客户RDD
  def getHistoryNoYqCustomRDD(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[String] = {
   return getBankBlanceCustomRdd(baseRdd).subtract(getMaxYqOneToThreeCustomRdd(baseRdd)).subtract(getMaxYqGreatFourCustomRdd(baseRdd)).cache()
  }


  //****************************************************************************************************************
  /*
  *
  *1.sum (case when posting_dte>today()-365 then tran_amt_paid else 0)as tranamt_tot by member_code
  *2.if tranamt_tot>0 则历史有还款；if tranamt_tot<=0 则历史无还款
  *
  *
  *
  * */
  //历史还款用户RDD
  // 思路：只要近1年内用户有一次tran_amt_paid>0则用户历史有还款
  //先筛选出一年内所有有还款的用户，然后与历史无逾期的用户RDD做交集
  def getHistoryRepaymentCustomRdd(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[String] = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    return baseRdd.mapPartitions(itertor => itertor.
      filter(t => ((new Date().getTime - dateFormat.parse(t._8).getTime) / (1000 * 60 * 60 * 24) <= 365)
        && (Double.valueOf(t._6) > 0)).map(t => t._1)).
      distinct().intersection(getHistoryNoYqCustomRDD(baseRdd)).cache()
  }

  //历史有无还款用户RDD,
  //思路：先筛选出一年内所有记录的CUS_NMB，然后与历史无逾期的用户RDD做交集，再减去历史有还款用户的RDD
  def getHistoryNoRepaymentCustomRdd(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[String] = {
   return getHistoryNoYqCustomRDD(baseRdd).subtract(getHistoryRepaymentCustomRdd(baseRdd))

  }
}

