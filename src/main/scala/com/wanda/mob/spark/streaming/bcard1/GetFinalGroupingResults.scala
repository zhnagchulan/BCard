package com.wanda.mob.spark.streaming.bcard1

import java.io.FileWriter
import java.text.SimpleDateFormat

import com.wanda.mob.spark.streaming.event.impl.{CommonEvent, ScoreEvent}
import com.wanda.mob.spark.streaming.models.{Model_Four, Model_One, Model_Three, Model_Two}
import com.wanda.mob.spark.streaming.utils.{BCardModel, CustomClassing}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object GetFinalGroupingResults {
//  var session=SparkSession()
  //得到客群2打分结果
  def getGroupTwoResults(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]): RDD[Tuple4[String, String, String, String]] = {

    BCardModel.modelTwo(GetGroupedCustomAllData.getNoYqCustomDataRdd(baseRdd))
  }

  //得到客群3打分结果
  def getGroupThreeResults(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]): RDD[Tuple4[String, String, String, String]] = {
    BCardModel.modelThree(GetGroupedCustomAllData.getNoYqCustomDataRdd(baseRdd))
  }

  //得到客群123不入打分模型的结果
  def getGroupOne_Two_Three_Results(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]): RDD[Tuple4[String, String, String, String]] = {
    BCardModel.noScore(GetGroupedCustomAllData.getNoYqCustomDataRdd(baseRdd))
  }

  //得到客群4的打分结果
  def getGroupFounResults(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]): RDD[Tuple4[String, String, String, String]] = {
    Model_One.getHH(GetGroupedCustomAllData.getHistoryNoRepaymentCustomDataRdd(baseRdd))
  }

  //得到客群4的打分结果
  //  def getGroupFourResults():RDD[Tuple4[String,java.lang.Double,String,String]]= {
  //    return Model_One.getHH(GetGroupedCustomAllData.getHistoryNoRepaymentCustomDataRdd())
  //  }

  //得到客群5的打分结果
  def getGroupFiveResults(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]): RDD[Tuple4[String, String, String, String]] = {
    Model_Two.scoring("客群5",GetGroupedCustomAllData.getHistoryRepaymentCustomDataRdd(baseRdd)).mapPartitions(
      itertor => itertor.map(t => (t._1, t._2.toString, "5", t._3))

    )
  }

  //得到客群6的打分结果
  def getGroupSixResults(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]): RDD[Tuple4[String, String, String, String]] = {
    Model_Three.scoring("客群6",GetGroupedCustomAllData.getMaxYqOneToThreeCustomDataRdd(baseRdd)).mapPartitions(
      itertor => itertor.map(t => (t._1, t._2.toString, "6", t._3))

    )
  }

  //得到客群7的分类结果
  def getGroupSevenResults(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]): RDD[Tuple4[String, String, String, String]] = {
    Model_Four.scoring(GetGroupedCustomAllData.getMaxYqGreatFourCustomDataRdd(baseRdd)).mapPartitions(
      itertor => itertor.map(t => (t._1, t._2.toString, "7", t._3))

    )
  }

  //得到客群8的分类结果
  def getGroupEightResults(baseRdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String)]): RDD[Tuple4[String, String, String, String]] = {
    val SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    GetGroupedCustomAllData.getYqCustomDataRdd(baseRdd).mapPartitions(itertor => itertor.map(
      t => (t._1, "NULL", "8", t._2.maxBy(t => SDF.parse(t.LST_UPD_TIME)).LST_UPD_TIME)
    ))

  }

  //得到全部数据的拼接结果集
  def resultsUnion(rdd:RDD[CommonEvent],globalDate:Broadcast[String]): RDD[ScoreEvent] = {
    val SDF = new SimpleDateFormat("yyyy-MM-dd")
    val baseRdd=rdd.mapPartitions(
          itertor => {
            itertor.filter(x=> !(x.CASH_AMT=="0" || x.CASH_AMT==null)).map(
              t => {
                (t.CUST_NBR, t.ACCOUNT_NMBR, t.PYMT_FLAG, t.DELQ_STATUS
                  , t.PRINCIPAL, t.TRAN_AMT_PAID, t.CASH_AMT, t.POSTING_DTE
                  , t.STATUS, t.TRANSACTION_TYPE, t.PAYMENT_DTE, t.LST_UPD_TIME
                  , t.AUDIT_TIME)
              }
            )
          }
        ).cache()
    val baseRddFilter=rdd.mapPartitions(
      itertor => {
        itertor.filter(x=>x.CASH_AMT=="0" || x.CASH_AMT==null).map(
          t => (t.CUST_NBR,t.LST_UPD_TIME)
        )
      }
    ).reduceByKey((x,y)=>if (x<y) y else x)
      .map(x=>(x._1,"NULL","1",x._2))
      .cache()
    val union_one = getGroupOne_Two_Three_Results(baseRdd).union(getGroupTwoResults(baseRdd)).cache()
    val union_two = getGroupThreeResults(baseRdd).union(getGroupFounResults(baseRdd)).cache()
    val union_three = getGroupFiveResults(baseRdd).union(getGroupSixResults(baseRdd)).cache()
    val union_four = getGroupSevenResults(baseRdd).union(getGroupEightResults(baseRdd)).cache()
    val union_five = union_one.union(union_two).cache()
    val union_six = union_three.union(union_four).cache()
    val union_seven = union_five.union(union_six).union(baseRddFilter).cache()

//
    //测试当前有余额用户
    getGroupTwoResults(baseRdd).map(x => {
      val ss = new ScoreEvent()
      ss.CUST_NBR = x._1
      ss.DATE_D = globalDate.value
      ss.GROUPS = x._3.toInt
      ss.SCORE = x._2
      ss.LST_UPD_TIME = x._4
      ss
    })
  }

}