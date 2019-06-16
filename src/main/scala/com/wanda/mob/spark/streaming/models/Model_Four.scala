package com.wanda.mob.spark.streaming.models

import java.text.SimpleDateFormat
import java.util.Date
import java.lang.Double

import com.wanda.mob.spark.streaming.event.impl.CommonEvent
import com.wanda.mob.spark.streaming.utils.CustomClassing
import org.apache.spark.rdd.RDD
import com.wanda.mob.spark.streaming.main.Streaming.sc
import com.wanda.mob.spark.streaming.main.New_Offline.sc


class Model_Four (){
  var delq_paytm_diff_30:Double = _
  var latest_oday_max:Double= _
  var min_yqh_day:Double = _
  var shidong_rate0:Double=_
  var yqhk_cnt_ratio_90:Double=_
  var timeStamp:String=_
}


object Model_Four{
  val Intercept:Double= 0.093
  def scoring(customData:RDD[Tuple2[String,Iterable[CommonEvent]]]) :RDD[Tuple3[String,Double,String]]={

    val SDF =new SimpleDateFormat("yyyy-MM-dd")
    val today=SDF.parse(SDF.format(new Date()))
    //customData.cache()
    //****************************************************************************************
    val customParRdd=customData.mapPartitions(itertor => itertor.map(t => {
      val par = new Model_Four()

      //OK****************************************************************************************
      //计算delq_paytm_diff_30
      val list= t._2.filter(t=>
        (today.getTime-SDF.parse(t.PAYMENT_DTE).getTime)/(1000*60*60*24)<=30
          &&t.DELQ_STATUS.contains("1"))
        .toList.sortBy(t=>SDF.parse(t.PAYMENT_DTE))
      //30天之前的最后一次逾期的记录
      val last_30=t._2.filter(t=>
        (today.getTime-SDF.parse(t.PAYMENT_DTE).getTime)/(1000*60*60*24)>30
          && t.DELQ_STATUS.contains("1"))
        .toList
      //30天之内是否有逾期的记录，list.size>0表示有,否则表示无，用-1标识
      if(list.nonEmpty&&(list.size+last_30.size)>1){
        var listdiff30:List[Long]=List()
        if(last_30.nonEmpty) {
          val firstelement30 = (SDF.parse(list.head.PAYMENT_DTE).getTime - SDF.parse(last_30.maxBy(t=>SDF.parse(t.PAYMENT_DTE)).PAYMENT_DTE).getTime) / (1000 * 60 * 60 * 24)
          listdiff30=math.abs(firstelement30)::listdiff30
        }

        for (i<- 0 to list.size-2){
          val diff= (SDF.parse(list(i+1).PAYMENT_DTE).getTime-SDF.parse(list(i).PAYMENT_DTE).getTime)/(1000*60*60*24)
          listdiff30=math.abs(diff)::listdiff30
          // println("diff30:"+diff)
        }
        //println("diff30:"+listdiff)
        par.delq_paytm_diff_30=listdiff30.min.toDouble
        // println("min:"+listdiff30.minBy(x=>x))
      }else{
        par.delq_paytm_diff_30= -1.0
      }

      //OK****************************************************************************************
      //计算min_yqh_day（最晚逾期还款至今天数）

      val min=t._2.filter(t=> t.DELQ_STATUS.contains("1")).minBy(t=>today.getTime-SDF.parse(t.LST_UPD_TIME).getTime)
      // .toList.sortBy(t=>SDF.parse(t.LST_UPD_TIME).getTime-SDF.parse(t.PAYMENT_DTE).getTime).head
      var tempmin=0.0
      if(min!=null) {
        tempmin = (today.getTime-SDF.parse(min.LST_UPD_TIME).getTime) / (1000 * 60 * 60 * 24)
      }
      par.min_yqh_day=Double.valueOf(tempmin)



      //OK****************************************************************************************
      //计算shidong_rate0
      //思路：累加每笔交易实动率
      par.shidong_rate0=t._2
        .map(t => ((t.ACCOUNT_NMBR, t.PRINCIPAL, t.CASH_AMT), Double.valueOf(t.TRAN_AMT_PAID))).groupBy(t=>t._1)
        .map(t=>(t._1._1,(Double.valueOf(t._1._2)-t._2.toList.
          map(t=>t._2).aggregate(0.0)({ (sum, ch) => sum + ch.toDouble },
          { (p1, p2) => p1 + p2 }))/Double.valueOf(t._1._3))).values
        .toList.aggregate(0.0)({ (sum, ch) => sum + ch }, { (p1, p2) => p1 + p2 })
      //OK****************************************************************************************
      //计算yqhk_cnt_ratio_90（90天内的逾期还款次数/90天内的还款次数）(除去提前还款)
      val tmp1=Double.valueOf(t._2.filter(t => (today.getTime - SDF.parse(t.PAYMENT_DTE).getTime) / (1000 * 60 * 60 * 24) <90
        &&(today.getTime - SDF.parse(t.PAYMENT_DTE).getTime) / (1000 * 60 * 60 * 24) >=0&& t.DELQ_STATUS.contains("1") && Double.valueOf(t.TRAN_AMT_PAID)>0).toList.size)
      val tmp2=  Double.valueOf(t._2.filter(t => (today.getTime - SDF.parse(t.PAYMENT_DTE).getTime) / (1000 * 60 * 60 * 24) <90
        &&(today.getTime - SDF.parse(t.PAYMENT_DTE).getTime) / (1000 * 60 * 60 * 24) >=0 &&Double.valueOf(t.TRAN_AMT_PAID)>0).toList.size)
      if(tmp1==0 || tmp2==0){
        par.yqhk_cnt_ratio_90=0.0
      }else {
        par.yqhk_cnt_ratio_90 = tmp1/tmp2
      }
      //OK****************************************************************************************
      //计算latest_oday_max（最近一笔交易时间以后的记录中最大逾期天数）
      val recentDate=SDF.parse(t._2.maxBy(t=>SDF.parse(t.POSTING_DTE)).POSTING_DTE)
      val temp_max=t._2.filter(t=> t.DELQ_STATUS.contains("1") &&
        SDF.parse(t.PAYMENT_DTE).getTime-recentDate.getTime>=0
        && SDF.parse(t.PAYMENT_DTE).getTime-today.getTime<=0
      ).map(t=>SDF.parse(t.LST_UPD_TIME).getTime-SDF.parse(t.PAYMENT_DTE).getTime)

      if(temp_max.isEmpty){par.latest_oday_max=0.0}else{
        par.latest_oday_max=(temp_max.max/(1000*60*60*24)).toDouble} //****************************************************************************************
      //计算timeStamp
      par.timeStamp=t._2.maxBy(t=>SDF.parse(t.LST_UPD_TIME)).LST_UPD_TIME
      (t._1, par)}
    ))

    val scoreRdd=customParRdd.mapPartitions(itertor=>itertor.map(t=>{
//      println(t._1+"------t._2.yqhk_cnt_ratio_90:"+t._2.yqhk_cnt_ratio_90)
//      println(t._1+"------t._2.shidong_rate0:"+t._2.shidong_rate0)
//      println(t._1+"------t._2.delq_paytm_diff_30:"+t._2.delq_paytm_diff_30)
//      println(t._1+"------t._2.min_yqh_day:"+t._2.min_yqh_day)
//      println(t._1+"------t._2.latest_oday_max:"+t._2.latest_oday_max)
      //****************************************************************************************
      //计算delq_paytm_diff_30minbox的分数
      if(t._2.delq_paytm_diff_30<0){
        t._2.delq_paytm_diff_30= -0.607153609
      }else if(t._2.delq_paytm_diff_30<=6){
        t._2.delq_paytm_diff_30= 0.585527035
      }else if(t._2.delq_paytm_diff_30<=15){
        t._2.delq_paytm_diff_30=0.305285164
      }else if(t._2.delq_paytm_diff_30<=31){
        t._2.delq_paytm_diff_30=0.161400525
      }else{
        t._2.delq_paytm_diff_30= -0.094515962
      }
      //****************************************************************************************
      //计算latest_oday_max的分数
      if(t._2.latest_oday_max<=0){
        t._2.latest_oday_max= -0.320998533
      }else if(t._2.latest_oday_max<=4){
        t._2.latest_oday_max= -0.437743576
      }else if(t._2.latest_oday_max<=10){
        t._2.latest_oday_max=0.127643699
      }else{
        t._2.latest_oday_max=0.707789905
      }
      //****************************************************************************************
      //计算min_yqh_day的分数
      if(t._2.min_yqh_day<=7 ){
        t._2.min_yqh_day= 0.221400646
      }else if(t._2.min_yqh_day<=15) {
        t._2.min_yqh_day= 0.124255484
      }else if(t._2.min_yqh_day<=60){
        t._2.min_yqh_day= -0.090518812
      }else{
        t._2.min_yqh_day= -0.389390429
      }
      //****************************************************************************************
      //计算shidong_rate0的分数
      if(t._2.shidong_rate0<=0.2){
        t._2.shidong_rate0= -1.450492973
      }else if(t._2.shidong_rate0<=0.4) {
        t._2.shidong_rate0= -0.471852575
      }else if(t._2.shidong_rate0<=0.6) {
        t._2.shidong_rate0= -0.012146893
      }else if(t._2.shidong_rate0<=0.8) {
        t._2.shidong_rate0= 0.382049215
      }else{
        t._2.shidong_rate0=0.467467963
      }
      //****************************************************************************************
      //计算yqhk_cnt_ratio_90的分数
      if(t._2.yqhk_cnt_ratio_90<=0){
        t._2.yqhk_cnt_ratio_90= -0.717319766
      }else if(t._2.yqhk_cnt_ratio_90<=0.2){
        t._2.yqhk_cnt_ratio_90= -0.299686296
      }else if(t._2.yqhk_cnt_ratio_90<=0.4) {
        t._2.yqhk_cnt_ratio_90= -0.105595553
      }else if(t._2.yqhk_cnt_ratio_90<=0.6) {
        t._2.yqhk_cnt_ratio_90= 0.175004632
      }else if(t._2.yqhk_cnt_ratio_90<=0.8) {
        t._2.yqhk_cnt_ratio_90= 0.27687999
      }else{
        t._2.yqhk_cnt_ratio_90=0.441641459
      }

      //****************************************************************************************
      //计算sum打分
      val totalScore1=t._2.yqhk_cnt_ratio_90+t._2.shidong_rate0
      val totalScore2=t._2.delq_paytm_diff_30+t._2.min_yqh_day
      val totalScore3=t._2.latest_oday_max+totalScore1
      val totalScore=totalScore3+totalScore2
//      println("yqhk_cnt_ratio_90:"+t._2.yqhk_cnt_ratio_90)
//      println("t._2.shidong_rate0:"+t._2.shidong_rate0)
//      println("t._2.delq_paytm_diff_30:"+t._2.delq_paytm_diff_30)
//      println("t._2.min_yqh_day:"+t._2.min_yqh_day)
//      println("t._2.latest_oday_max:"+t._2.latest_oday_max)
//      println("totalScore"+totalScore)
      //计算B_score
      val B_score=500+50/Math.log(2)*(-(totalScore+Intercept))

      //println("B_score"+B_score)
      (t._1, Double.valueOf(B_score.formatted("%.5f")),t._2.timeStamp)
    }))
    //****************************************************************************************
    //返回（CUST_NUM,B_score,timeStamp）Rdd
    return scoreRdd
  }



}

