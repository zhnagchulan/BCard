package com.wanda.mob.spark.streaming.bcard1

import java.text.SimpleDateFormat

import com.wanda.mob.spark.streaming.event.impl.{CommonEvent, ScoreEvent}
import com.wanda.mob.spark.streaming.models.{Model_Four, Model_One, Model_Three, Model_Two}
import com.wanda.mob.spark.streaming.utils.{BCardModel, CustomClassing}
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import java.util.Date

object GetFinalGroupingResults {

  def getGroupTwoResults(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[Tuple4[String, String, String, String]] = {

    BCardModel.modelTwo(GetGroupedCustomAllData.getNoYqCustomDataRdd(baseRdd))
  }

  //得到客群3打分结果
  def getGroupThreeResults(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[Tuple4[String, String, String, String]] = {

    BCardModel.modelThree(GetGroupedCustomAllData.getNoYqCustomDataRdd(baseRdd))
  }

  //得到客群123不入打分模型的结果
  def getGroupOne_Two_Three_Results(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[Tuple4[String, String, String, String]] = {
    BCardModel.noScore(GetGroupedCustomAllData.getNoYqCustomDataRdd(baseRdd))
  }

  //得到客群4的打分结果
  def getGroupFourResults(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[Tuple4[String, String, String, String]] = {
    Model_One.getHH(GetGroupedCustomAllData.getHistoryNoRepaymentCustomDataRdd(baseRdd))
  }

  //得到客群4的打分结果
  //  def getGroupFourResults():RDD[Tuple4[String,java.lang.Double,String,String]]= {
  //    return Model_One.getHH(GetGroupedCustomAllData.getHistoryNoRepaymentCustomDataRdd())
  //  }

  //得到客群5的打分结果
  def getGroupFiveResults(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[Tuple4[String, String, String, String]] = {
    Model_Two.scoring("客群5", GetGroupedCustomAllData.getHistoryRepaymentCustomDataRdd(baseRdd)).mapPartitions(
      itertor => itertor.map(t => (t._1, t._2.toString, "5", t._3))

    )
  }

  //得到客群6的打分结果
  def getGroupSixResults(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[Tuple4[String, String, String, String]] = {
    Model_Three.scoring("客群6", GetGroupedCustomAllData.getMaxYqOneToThreeCustomDataRdd(baseRdd)).mapPartitions(
      itertor => itertor.map(t => (t._1, t._2.toString, "6", t._3))

    )
  }

  //得到客群7的分类结果
  def getGroupSevenResults(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[Tuple4[String, String, String, String]] = {
    Model_Four.scoring(GetGroupedCustomAllData.getMaxYqGreatFourCustomDataRdd(baseRdd)).mapPartitions(
      itertor => itertor.map(t => (t._1, t._2.toString, "7", t._3))

    )
  }

  //得到客群8的分类结果
  def getGroupEightResults(baseRdd: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)]): RDD[Tuple4[String, String, String, String]] = {
    val SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    GetGroupedCustomAllData.getYqCustomDataRdd(baseRdd).mapPartitions(itertor => itertor.map(
      t => (t._1, null, "8", t._2.maxBy(t => SDF.parse(t.LST_UPD_TIME)).LST_UPD_TIME)
    ))

  }



  //跑实时数据，得到全部数据的拼接结果集
  def resultsUnion(rdd: RDD[CommonEvent]): RDD[ScoreEvent] = {
    val SDF = new SimpleDateFormat("yyyy-MM-dd")
    val rdd_GE90 = rdd.filter(t =>
      t.AUDIT_TIME != null &&
        (SDF.parse(SDF.format(new Date())).getTime - SDF.parse(t.AUDIT_TIME).getTime) / (1000 * 60 * 60 * 24) >= 90)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val baseRdd = rdd_GE90.mapPartitions(
      itertor => {
        itertor.filter(x => !(x.PRINCIPAL == null || x.CASH_AMT == null || x.CASH_AMT == "0")).map(
          t => {
            (t.MEMBER_CODE, t.ACCOUNT_NMBR, t.PYMT_FLAG, t.DELQ_STATUS
              , t.PRINCIPAL, t.TRAN_AMT_PAID, t.CASH_AMT, t.POSTING_DTE
              , t.STATUS, t.TRANSACTION_TYPE, t.PAYMENT_DTE, t.LST_UPD_TIME
              , t.AUDIT_TIME)
          }
        )
      }
    ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val baseRddFilter: RDD[Tuple4[String, String, String, String]] = rdd_GE90.mapPartitions(
      itertor => {
        itertor.filter(x => x.PRINCIPAL == null || x.CASH_AMT == null || x.CASH_AMT == "0"
        )
          .map(t => {
//            println((t.CUST_NBR, t.LST_UPD_TIME: String))
            (t.MEMBER_CODE, t.LST_UPD_TIME: String)
          }
          )
      }
    ).reduceByKey((x, y) => {
      if (x != null && y != null) {
        if (x < y) y else x
      } else {
        null: String
      }
    }).map(x => (x._1, null: String, "1", x._2))
      //union属于客群1的僵尸用户
      .union(rdd.filter(t =>
      t.AUDIT_TIME == null).map(x => (x.MEMBER_CODE, null: String, "1", null:String)).distinct())
    rdd_GE90.unpersist()
    val union_one = getGroupOne_Two_Three_Results(baseRdd).union(getGroupTwoResults(baseRdd))
    val union_two = getGroupThreeResults(baseRdd).union(getGroupFourResults(baseRdd))
    val union_three = getGroupFiveResults(baseRdd).union(getGroupSixResults(baseRdd))
    val union_four = getGroupSevenResults(baseRdd).union(getGroupEightResults(baseRdd))
    val union_five = union_one.union(union_two)
    val union_six = union_three.union(union_four)
    // getGroupNineResults(rdd_Less90)
    val union_seven = union_five.union(union_six).union(baseRddFilter).cache()

    //    val a25=getGroupTwoResults(baseRdd).union(getGroupFiveResults(baseRdd))
    //测试当前有余额用户
    baseRdd.unpersist()
    union_seven.map(x => {
      val ss = new ScoreEvent()
      ss.MEMBER_CODE = x._1
      ss.DATE_D = SDF.format(SDF.parse(x._4).getTime)
      ss.GROUPS = x._3.toInt
      ss.SCORE = x._2
      ss.LST_UPD_TIME = x._4
      //      println("final="+ss.CUST_NBR,ss.LST_UPD_TIME)
      ss
    }).cache()

  }

  //跑离线数据
  def resultsUnion(rdd: RDD[CommonEvent], ssd: Broadcast[String]): RDD[ScoreEvent] = {
    val SDF = new SimpleDateFormat("yyyy-MM-dd")
    val rdd_GE90 = rdd.filter(t =>
      t.AUDIT_TIME != null
        && (SDF.parse(SDF.format(new Date())).getTime - SDF.parse(t.AUDIT_TIME).getTime) / (1000 * 60 * 60 * 24) >= 90)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val baseRdd = rdd_GE90.mapPartitions(
      itertor => {
        itertor.filter(x => !(x.PRINCIPAL == null || x.CASH_AMT == null || x.CASH_AMT == "0")).map(
          t => {
//            println((t.CUST_NBR, t.ACCOUNT_NMBR, t.PYMT_FLAG, t.DELQ_STATUS
//              , t.PRINCIPAL, t.TRAN_AMT_PAID, t.CASH_AMT, t.POSTING_DTE
//              , t.STATUS, t.TRANSACTION_TYPE, t.PAYMENT_DTE, t.LST_UPD_TIME
//              , t.AUDIT_TIME))


            (t.MEMBER_CODE, t.ACCOUNT_NMBR, t.PYMT_FLAG, t.DELQ_STATUS
              , t.PRINCIPAL, t.TRAN_AMT_PAID, t.CASH_AMT, t.POSTING_DTE
              , t.STATUS, t.TRANSACTION_TYPE, t.PAYMENT_DTE, t.LST_UPD_TIME
              , t.AUDIT_TIME)
          }
        )
      }
    ).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val baseRddFilter: RDD[Tuple4[String, String, String, String]] = rdd_GE90.mapPartitions(
      itertor => {
        itertor.filter(x => x.PRINCIPAL == null || x.CASH_AMT == null || x.CASH_AMT == "0")
          .map(t => {
            //println((t.CUST_NBR, t.LST_UPD_TIME: String))
            (t.MEMBER_CODE, t.LST_UPD_TIME: String)
          }
          )
      }
    ).reduceByKey((x, y) => {
      if (x != null && y != null) {
        if (x < y) y else x
      } else {
        null: String
      }
    }).map(x => (x._1, null: String, "1", x._2))
      //union属于客群1的僵尸用户
      .union(rdd.filter(t =>
      t.AUDIT_TIME == null).map(x => (x.MEMBER_CODE, null: String, "1", null:String)).distinct())
    rdd_GE90.unpersist()


    val union_one = getGroupOne_Two_Three_Results(baseRdd).union(getGroupTwoResults(baseRdd))
    val union_two = getGroupThreeResults(baseRdd).union(getGroupFourResults(baseRdd))
    val union_three = getGroupFiveResults(baseRdd).union(getGroupSixResults(baseRdd))
    val union_four = getGroupSevenResults(baseRdd).union(getGroupEightResults(baseRdd))
    val union_five = union_one.union(union_two)
    val union_six = union_three.union(union_four)
    val union_seven = union_five.union(union_six).union(baseRddFilter)
    val union_eight=union_seven.union(KequnNan.kequn_RDD9()).cache()

//    val ddd=getGroupFiveResults(baseRdd).union(getGroupFourResults(baseRdd))
    baseRdd.unpersist()
    union_eight.map(x => {
      val ss = new ScoreEvent()

      ss.MEMBER_CODE = x._1
      ss.DATE_D = ssd.value
      ss.GROUPS = x._3.toInt
      ss.SCORE = x._2
      ss.LST_UPD_TIME = x._4
//      println(ss.MEMBER_CODE+"_"+ss.GROUPS+"_"+ss.SCORE+"_"+ss.LST_UPD_TIME+"_"+"Test")
      ss
    }).cache()

  }

}