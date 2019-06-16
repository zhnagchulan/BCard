package com.wanda.mob.spark.streaming.models

import java.text.SimpleDateFormat
import java.util.{Date, LinkedList}
import java.lang.Double

import com.wanda.mob.spark.streaming.event.impl.CommonEvent
import org.apache.spark.rdd.RDD
import java.util.Calendar


class Model_Two() {
  var fst_trade_shidong: Double = _
  var loan_pain_tot: Double = _
  var post_23_06_prata6: Double = _
  var post_cnt1: Double = _
  var shidong_avg_3mon: Double = _
  var shidong_avg_apr: Double = _
  var timeStamp: String = _

}

object Model_Two {

  val Intercept: Double = -3.1959

  def scoring(difFlag:String,customData: RDD[Tuple2[String, Iterable[CommonEvent]]]): RDD[Tuple3[String, Double, String]] = {
    val SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormatByDay = new SimpleDateFormat("yyyy-MM-dd")
    //val today = SDF.parse(SDF.format(new Date()))

  //  customData.cache()
    //****************************************************************************************
    val customParRdd = customData.mapPartitions(itertor => itertor.map(t => {
      val par = new Model_Two()
      var todayToday = dateFormatByDay.parse(dateFormatByDay.format(new Date()))
      if(difFlag.endsWith("客群2")){
        todayToday=dateFormatByDay.parse(t._2.filter(t=>Double.valueOf(t.TRAN_AMT_PAID)>0.0)
          .maxBy(t=>SDF.parse(t.LST_UPD_TIME)).LST_UPD_TIME)

      }
      //****************************************************************************************
      //计算fst_trade_shidong
      //思路：fst_trade_shidong=第一笔交易的PRINCIPAL/CASH_AMT
      val min = t._2.minBy(t => SDF.parse(t.POSTING_DTE))

      if (min == null) {
        par.fst_trade_shidong = -1.0
      } else {
        par.fst_trade_shidong = Double.valueOf(min.PRINCIPAL) / Double.valueOf(min.CASH_AMT)
      }
      //OK****************************************************************************************
      //计算post_23_06_prata6
      //思路：先求180天以内的PRINCIPAL累加做分母，后求近6个月posting_dte在23-6点的principal的累加和做分子
      val less180 = t._2.groupBy(t => t.ACCOUNT_NMBR).map(t => (t._1, t._2.minBy(t => t.PAYMENT_DTE)))
        .map(t => (t, Double.valueOf(todayToday.getTime - SDF.parse(t._2.POSTING_DTE).getTime) / (1000 * 60 * 60 * 24)))
        .filter(t => t._2 <= 180)

      val isZero = less180.map(t => Double.valueOf(t._1._2.PRINCIPAL)).toList.aggregate(0.0)({ (sum, ch) => sum + ch.toDouble }, { (p1, p2) => p1 + p2 })
      if (isZero != 0) {
       val fenzi= less180.filter(t => SDF.parse(t._1._2.POSTING_DTE).getHours>=23 || SDF.parse(t._1._2.POSTING_DTE).getHours <= 6).map(t => Double.valueOf(t._1._2.PRINCIPAL)).toList.aggregate(0.0)({ (sum, ch) => sum + ch.toDouble }, { (p1, p2) => p1 + p2 })
        par.post_23_06_prata6 = (fenzi / isZero)
      } else {
        par.post_23_06_prata6 = -1.0
      }
      //OK****************************************************************************************
      //计算post_cnt1
      //思路：统计30天内的ACCOUNT_NMBR
      par.post_cnt1 = t._2.filter(t => (todayToday.getTime - SDF.parse(t.POSTING_DTE).getTime) / (1000 * 60 * 60 * 24) <= 30)
        .map(t => t.ACCOUNT_NMBR).toSet.size.toDouble

      //OK****************************************************************************************
      //计算loan_pain_tot
      //思路：借款日期的集合与还款日期的集合的交集的集合即为又借款又还款的日期集合
      //从借款日期集合中拿出交集中的日期（避免去重借款日期）对这些日期进行count
      val days = (todayToday.getTime - dateFormatByDay.parse(min.POSTING_DTE).getTime) / (1000 * 60 * 60 * 24)
      if (days != 0) {
        val hk_Date= t._2.filter(t=>Double.valueOf(t.TRAN_AMT_PAID)>0&&dateFormatByDay.parse(t.LST_UPD_TIME).getTime<=todayToday.getTime).map(t=>dateFormatByDay.parse(t.LST_UPD_TIME).toString).toSet
        val jk_Date = t._2.filter(t=>Double.valueOf(t.PRINCIPAL)>0&&dateFormatByDay.parse(t.POSTING_DTE).getTime<=todayToday.getTime).map(t=>(t.ACCOUNT_NMBR,dateFormatByDay.parse(t.POSTING_DTE).toString)).toSet
        var count = 0
        for (k <- (hk_Date).intersect(jk_Date.map(t=>t._2))) {
          jk_Date.foreach(t=>{
            if(t._2.equals(k)){count=count+1}
          })}
        par.loan_pain_tot = Double.valueOf(count) / Double.valueOf(days)
//        println(t._1+"分子："+count)
//        println(t._1+"分母："+days)
//         println(t._1+"jk："+
//         jk_Date.map(t=>t._2).mkString(","))
//        println(t._1+"hk："+
//          hk_Date.mkString(","))

      } else {
        par.loan_pain_tot = -1.0
      }
      //OK****************************************************************************************
      //计算shidong_avg_3mon
      /*思路借款和：（1）求出90天内的借款和
      *        (2)求出90天内还款和
      *        （3）累加90天（（借款和-还款和）/ CASH_AMT）
      */

      var shiDongRateSumFor90: Double = 0.0
      //90天内Posttingdate集合
     // println(t._1+"是否有数据："+t._2.size)
      val postDTESet_90 = t._2.filter(t => (todayToday.getTime - dateFormatByDay.parse(t.POSTING_DTE).getTime)/(24 * 60 * 60 * 1000)<= 90 ).map(t => dateFormatByDay.parse(t.POSTING_DTE)).toSet
     //println(t._1+"jiekjihe"+postDTESet_90.size)
      //90天内last_update_time集合
      val lastUpdateDTESet_90 = t._2.filter(t => (todayToday.getTime - dateFormatByDay.parse(t.LST_UPD_TIME).getTime )/( 24 * 60 * 60 * 1000)<= 90 ).map(t => dateFormatByDay.parse(t.LST_UPD_TIME)).toSet
    //println(t._1+"huankjihe"+lastUpdateDTESet_90.size)
      //Posttingdate集合Unionlast_update_time集合求最小Date是否有数据
      val minDate_90 = lastUpdateDTESet_90.union(postDTESet_90)
      var endDate_90 = 89

      if (minDate_90.nonEmpty) {
        endDate_90 = ((todayToday.getTime - minDate_90.min.getTime) / (1000 * 60 * 60 * 24)).toInt
      }
      for (i <- 0 to endDate_90) {
        //筛选出指定时间范围之内的还款记录
        val temp_LastUpDate_90 =  t._2.filter(t => ((todayToday.getTime - dateFormatByDay.parse(t.LST_UPD_TIME).getTime) / (1000 * 60 * 60 * 24))-i >= 0&&Double.valueOf(t.TRAN_AMT_PAID)>0).map(t=>Double.valueOf(t.TRAN_AMT_PAID))
        //筛选出指定时间范围之内的借款记录
        val temp_PostDate_90 =  t._2.filter(t => ((todayToday.getTime  - dateFormatByDay.parse(t.POSTING_DTE).getTime) / (1000 * 60 * 60 * 24))-i >= 0).map(t=>(t.ACCOUNT_NMBR,t.PRINCIPAL)).toSet[(String,String)].toList.map(t=>Double.valueOf(t._2))
        //求借款总和
        var pri_sum_90=0.0
        temp_PostDate_90.foreach(t=>pri_sum_90=pri_sum_90+t)
        //求还款总和
        var pai_sum_90=0.0
        temp_LastUpDate_90.foreach(t=>pai_sum_90=pai_sum_90+t)
        //求一天的shidongRate
        var shi_90=(pri_sum_90-pai_sum_90)/Double.valueOf(t._2.toList.head.CASH_AMT)
        //累加每天的shidongRate
        shiDongRateSumFor90= shiDongRateSumFor90+ shi_90
      }
      //求90天内的平均shidongRate
      par.shidong_avg_3mon = shiDongRateSumFor90 / 90
      //OK****************************************************************************************
      //  计算shidong_avg_apr
      //    思路：同shidong_avg_3mon
      var j = 0
      var shiDongRateSumFor30: Double = 0.0
      //30天内Posttingdate集合
      val postDTESet_30 = t._2.filter(t => (todayToday.getTime - dateFormatByDay.parse(t.POSTING_DTE)
        .getTime)/(24 * 60 * 60 * 1000) <= 30 ).map(t => dateFormatByDay.parse(t.POSTING_DTE)).toSet
      //30天内last_update_time集合
      val lastUpdateDTESet_30 = t._2.filter(t => (todayToday.getTime - dateFormatByDay.parse(t.LST_UPD_TIME)
        .getTime )/(24 * 60 * 60 * 1000)<= 30 ).map(t => dateFormatByDay.parse(t.LST_UPD_TIME)).toSet
      //Posttingdate集合Unionlast_update_time集合求最小Date
      val minDate_30 = lastUpdateDTESet_30.union(postDTESet_30)
      var endDate_30 = 29
      if (minDate_30 .nonEmpty) {
       // println("停止时间："+SDF.format(minDate_30.min))
        endDate_30 = ((todayToday.getTime - minDate_30.min.getTime) / (1000 * 60 * 60 * 24)).toInt
      }

      for (j <- 0 to endDate_30) {
        //筛选出指定时间范围之内的还款记录
        val temp_LastUpDate =  t._2.filter(t => ((todayToday.getTime  - dateFormatByDay.parse(t.LST_UPD_TIME).getTime) / (1000 * 60 * 60 * 24))-j >= 0&&Double.valueOf(t.TRAN_AMT_PAID)>0).map(t=>Double.valueOf(t.TRAN_AMT_PAID))
        //筛选出指定时间范围之内的借款记录
        val temp_PostDate =  t._2.filter(t => ((todayToday.getTime   - dateFormatByDay.parse(t.POSTING_DTE).getTime) / (1000 * 60 * 60 * 24))-j >= 0).map(t=>(t.ACCOUNT_NMBR,t.PRINCIPAL)).toSet[(String,String)].toList.map(t=>Double.valueOf(t._2))
        //求借款总和
        var pri_sum=0.0
        temp_PostDate.foreach(t=>pri_sum=pri_sum+t)
        //求还款总和
        var pai_sum=0.0
        temp_LastUpDate.foreach(t=>pai_sum=pai_sum+t)
        //求一天的shidongRate
        val shi=(pri_sum-pai_sum)/Double.valueOf(t._2.toList.head.CASH_AMT)
        //累加每天的shidongRate
        shiDongRateSumFor30= shiDongRateSumFor30+ shi
       //println(t._1+"当前向前第"+j+"天-----实动rate: "+shi+"----totalPri"+pri_sum+"-----totalPai"+pai_sum)

      }
      //求30天内的平均shidongRate
      par.shidong_avg_apr = shiDongRateSumFor30 / 30



      //****************************************************************************************
      //计算timeStamp
      par.timeStamp = t._2.maxBy(t => SDF.parse(t.LST_UPD_TIME)).LST_UPD_TIME
      (t._1, par)
    }
    ))

    val scoreRdd = customParRdd.mapPartitions(itertor => itertor.map(t => {
//            println(t._1+"-----------t._2.loan_pain_tot:  "+t._2.loan_pain_tot)
//            println(t._1+"-----------t._2.post_23_06_prata6:  "+t._2.post_23_06_prata6)
//            println(t._1+"-----------t._2.shidong_avg_apr  "+t._2.shidong_avg_apr)
//            println(t._1+"-----------t._2.fst_trade_shidong:  "+t._2.fst_trade_shidong)
//            println(t._1+"-----------t._2.post_cnt1 "+t._2.post_cnt1)
//            println(t._1+"-----------t._2.shidong_avg_3mon:  "+t._2.shidong_avg_3mon)
      //****************************************************************************************
      //计算fst_trade_shidongbox的分数
      if (t._2.fst_trade_shidong <= 0.24 && t._2.fst_trade_shidong >= -1) {
        t._2.fst_trade_shidong = -0.38901
      } else if (t._2.fst_trade_shidong <= 0.68) {
        t._2.fst_trade_shidong = -0.18977
      } else if (t._2.fst_trade_shidong <= 0.999) {
        t._2.fst_trade_shidong = -0.16666
      } else {
        t._2.fst_trade_shidong = 0.37817
      }
      //****************************************************************************************
      //计算loan_pain_tot_ratebox的分数
      if (t._2.loan_pain_tot <= 0.0025 && t._2.loan_pain_tot >= -1) {
        t._2.loan_pain_tot = -0.16345
      } else if (t._2.loan_pain_tot <= 0.005) {
        t._2.loan_pain_tot = -0.05703
      } else if (t._2.loan_pain_tot <= 0.01) {
        t._2.loan_pain_tot = -0.01812
      } else if (t._2.loan_pain_tot <= 0.016) {
        t._2.loan_pain_tot = 0.06305
      } else {
        t._2.loan_pain_tot = 0.10292
      }
      //****************************************************************************************
      //计算post_23_06_prate6box的分数
      if (t._2.post_23_06_prata6 == 0) {
        t._2.post_23_06_prata6 = -0.01237
      } else if (t._2.post_23_06_prata6 > 0) {
        t._2.post_23_06_prata6 = 0.13355
      } else {
        t._2.post_23_06_prata6 = -0.40210
      }
      //****************************************************************************************
      //计算post_cnt1box的分数
      if (t._2.post_cnt1 <= 0) {
        t._2.post_cnt1 = -0.17916
      } else if (t._2.post_cnt1 == 1) {
        t._2.post_cnt1 = 0.06719
      } else if (t._2.post_cnt1 == 2) {
        t._2.post_cnt1 = 0.19224
      } else if (t._2.post_cnt1 == 3) {
        t._2.post_cnt1 = 0.26890
      } else {
        t._2.post_cnt1 = 0.30239
      }
      //****************************************************************************************
      //计算shidong_avg_3monbox的分数
      if (t._2.shidong_avg_3mon <= 0.44 && t._2.shidong_avg_3mon >= -1) {
        t._2.shidong_avg_3mon = -0.34067
      } else if (t._2.shidong_avg_3mon <= 0.56) {
        t._2.shidong_avg_3mon = -0.12041
      } else if (t._2.shidong_avg_3mon <= 0.8) {
        t._2.shidong_avg_3mon = 0.03652
      } else if (t._2.shidong_avg_3mon <= 0.88) {
        t._2.shidong_avg_3mon = 0.09436
      } else {
        t._2.shidong_avg_3mon = 0.21441
      }
      //****************************************************************************************
      //计算shidong_avg_aprbox的分数
      if (t._2.shidong_avg_apr <= 0.5 && t._2.shidong_avg_apr >= -1) {
        t._2.shidong_avg_apr = -0.06899
      } else if (t._2.shidong_avg_apr <= 0.65) {
        t._2.shidong_avg_apr = -0.00834
      } else if (t._2.shidong_avg_apr <= 0.85) {
        t._2.shidong_avg_apr = 0.05061
      } else {
        t._2.shidong_avg_apr = 0.14252
      }
      //****************************************************************************************
      //计算sum打分
      val totalScore1 = t._2.loan_pain_tot + t._2.post_23_06_prata6
      val totalScore2 = t._2.shidong_avg_apr + t._2.fst_trade_shidong
      val totalScore3 = t._2.post_cnt1 + t._2.shidong_avg_3mon
      val totalScore4 = totalScore1 + totalScore2
      val totalScore = totalScore4 + totalScore3

//            println(t._1+"******t._2.loan_pain_tot:  "+t._2.loan_pain_tot)
//            println(t._1+"******t._2.post_23_06_prata6:  "+t._2.post_23_06_prata6)
//            println(t._1+"******t._2.shidong_avg_apr  "+t._2.shidong_avg_apr)
//            println(t._1+"******t._2.fst_trade_shidong:  "+t._2.fst_trade_shidong)
//            println(t._1+"******t._2.post_cnt1 "+t._2.post_cnt1)
//            println(t._1+"******t._2.shidong_avg_3mon:  "+t._2.shidong_avg_3mon)
//            println("value:"+t._2.latest_oday_max)
//            println("sum"+totalScore)
      //计算B_score
      val B_score = 500 + 50 / Math.log(2) * (-(totalScore + Intercept))

      (t._1, Double.valueOf(B_score.formatted("%.5f")), t._2.timeStamp)
    }))
    //****************************************************************************************
    //返回（CUST_NUM,B_score,timeStamp）Rdd
    return scoreRdd
  }


}
