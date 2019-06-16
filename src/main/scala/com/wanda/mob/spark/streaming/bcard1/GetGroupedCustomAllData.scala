package com.wanda.mob.spark.streaming.bcard1

import com.wanda.mob.spark.streaming.event.impl.CommonEvent
import com.wanda.mob.spark.streaming.utils.CustomClassing
import org.apache.spark.rdd.RDD
object GetGroupedCustomAllData {

  def getGroupByCust_NBR(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]):RDD[(String,CommonEvent)]={
    baseRdd.mapPartitions(itertor=>itertor.map(t=>{
      val temp=new CommonEvent()
      temp.CUST_NBR=t._1
      temp.ACCOUNT_NMBR = t._2
      temp.PYMT_FLAG= t._3
      temp. DELQ_STATUS= t._4
      temp. PRINCIPAL =t._5
      temp. TRAN_AMT_PAID =t._6
      temp. CASH_AMT = t._7
      temp. POSTING_DTE= t._8
      temp.STATUS =t._9
      temp.TRANSACTION_TYPE=t._10
      temp.PAYMENT_DTE = t._11
      temp. LST_UPD_TIME = t._12
      temp.AUDIT_TIME = t._13
      (temp.CUST_NBR,temp)
    }))

  }


  //获取逾期客户所有的数据
  def getYqCustomDataRdd(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]):RDD[Tuple2[String,Iterable[CommonEvent]]] ={
    return CustomClassing.getYqCustomRdd(baseRdd).mapPartitions(
      itertor=>itertor.map(t=>(t,1))).cogroup(getGroupByCust_NBR(baseRdd)).filter(t=>t._2._1.size>0)
      .mapPartitions(itertor=>itertor.map(
        t=>(t._1,t._2._2)
      )).cache()
  }
  //获取无逾期客户所有的数据
  def getNoYqCustomDataRdd(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]):RDD[Tuple2[String,Iterable[CommonEvent]]]= {
    return CustomClassing.getNoYqCustomRdd(baseRdd).mapPartitions(
      itertor=>itertor.map(t=>(t,1))).cogroup(getGroupByCust_NBR(baseRdd)).filter(t=>t._2._1.size>0)
      .mapPartitions(itertor=>itertor.map(
        t=>(t._1,t._2._2)
      )).cache()
  }
  //获取有余额用户的所有数据
  def getBankBlanceCustomDataRdd(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]):RDD[Tuple2[String,Iterable[CommonEvent]]]={
    return CustomClassing.getBankBlanceCustomRdd(baseRdd).mapPartitions(
      itertor=>itertor.map(t=>(t,1))).cogroup(getGroupByCust_NBR(baseRdd)).filter(t=>t._2._1.size>0)
      .mapPartitions(itertor=>itertor.map(
        t=>(t._1,t._2._2)
      )).cache()
  }
  //获取无余额用户的所有数据
  def getNoBankBlanceCustomDataRdd(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]):RDD[Tuple2[String,Iterable[CommonEvent]]]={
    return CustomClassing.getNoBankBlanceCustomRdd(baseRdd).mapPartitions(
      itertor=>itertor.map(t=>(t,1))).cogroup(getGroupByCust_NBR(baseRdd)).filter(t=>t._2._1.size>0)
      .mapPartitions(itertor=>itertor.map(
        t=>(t._1,t._2._2)
      )).cache()
  }
  //获取历史无逾期客户的所有数据
  def getHistoryNoYqCustomDataRDD(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]):RDD[Tuple2[String,Iterable[CommonEvent]]]={
    return CustomClassing.getHistoryNoYqCustomRDD(baseRdd).mapPartitions(
      itertor=>itertor.map(t=>(t,1))).cogroup(getGroupByCust_NBR(baseRdd)).filter(t=>t._2._1.size>0)
      .mapPartitions(itertor=>itertor.map(
        t=>(t._1,t._2._2)
      )).cache()
  }
  //获取历史逾期天数[1,3]的客户所有的数据
  def getMaxYqOneToThreeCustomDataRdd(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]):RDD[Tuple2[String,Iterable[CommonEvent]]]={
    return CustomClassing.getMaxYqOneToThreeCustomRdd(baseRdd).mapPartitions(
      itertor=>itertor.map(t=>(t,1))).cogroup(getGroupByCust_NBR(baseRdd)).filter(t=>t._2._1.size>0)
      .mapPartitions(itertor=>itertor.map(
        t=>(t._1,t._2._2)
      )).cache()

  }
  //获取历史逾期天数[4，+)的客户所有的数据
  def getMaxYqGreatFourCustomDataRdd(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]):RDD[Tuple2[String,Iterable[CommonEvent]]]={
    return CustomClassing.getMaxYqGreatFourCustomRdd(baseRdd).mapPartitions(
      itertor=>itertor.map(t=>(t,1))).cogroup(getGroupByCust_NBR(baseRdd)).filter(t=>t._2._1.size>0)
      .mapPartitions(itertor=>itertor.map(
        t=>(t._1,t._2._2)
      )).cache()
  }
  //获取历史无还款的用户所有的数据
  def getHistoryNoRepaymentCustomDataRdd(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]):RDD[Tuple2[String,Iterable[CommonEvent]]]={
    return CustomClassing.getHistoryNoRepaymentCustomRdd(baseRdd).mapPartitions(
      itertor=>itertor.map(t=>(t,1))).cogroup(getGroupByCust_NBR(baseRdd)).filter(t=>t._2._1.size>0)
      .mapPartitions(itertor=>itertor.map(
        t=>(t._1,t._2._2)
      )).cache()
  }
  //获取历史有还款的用户所有的数据
  def getHistoryRepaymentCustomDataRdd(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]):RDD[Tuple2[String,Iterable[CommonEvent]]]={
    return CustomClassing.getHistoryRepaymentCustomRdd(baseRdd).mapPartitions(
      itertor=>itertor.map(t=>(t,1))).cogroup(getGroupByCust_NBR(baseRdd)).filter(t=>t._2._1.size>0)
      .mapPartitions(itertor=>itertor.map(
        t=>(t._1,t._2._2)
      )).cache()
  }

}