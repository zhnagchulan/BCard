package com.wanda.mob.spark.streaming.models

import java.text.SimpleDateFormat
import java.util.Date
import java.lang.Double

import com.wanda.mob.spark.streaming.event.impl.CommonEvent
import com.wanda.mob.spark.streaming.main.Streaming.sc
import com.wanda.mob.spark.streaming.main.Offline.sc
import org.apache.spark.rdd.RDD


class Model_Three (){
  var delq_paytm_diff_30:Double = _
  var delq_paytm_diff_90:Double= _
  var max_oday_max3:Double = _
  var oday_max1:Double=_
  var timeStamp:String=_
}

object Model_Three {
  val Intercept:Double= -1.9748
  def scoring(difFlag:String,customData:RDD[Tuple2[String,Iterable[CommonEvent]]]) :RDD[Tuple3[String,Double,String]]={
    val SDF = new SimpleDateFormat("yyyy-MM-dd")

   // customData.cache()
    //****************************************************************************************
    val customParRdd=customData.mapPartitions(itertor => itertor.map(t => {
      val par = new Model_Three()
      var today = SDF.parse(SDF.format(new Date()))
      if(difFlag.endsWith("客群3")){
        today=SDF.parse(t._2.filter(t=>Double.valueOf(t.TRAN_AMT_PAID)>0.0)
          .maxBy(t=>SDF.parse(t.LST_UPD_TIME)).LST_UPD_TIME)
      }
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
      //计算delq_paytm_diff_90
      //思路：同上
      val list90= t._2.filter(t=>
        (today.getTime-SDF.parse(t.PAYMENT_DTE).getTime)/(1000*60*60*24)<=90
          && t.DELQ_STATUS.contains("1"))
        .toList.sortBy(t=>SDF.parse(t.PAYMENT_DTE))
      val last_90=t._2.filter(t=>
        (today.getTime-SDF.parse(t.PAYMENT_DTE).getTime)/(1000*60*60*24)>90
          && t.DELQ_STATUS.contains("1"))
        .toList
      // println(t._1+"listSize："+list90.size)
      if(list90.nonEmpty && (list90.size+last_90.size)>1) {
        var listdiff90:List[Long] = List()
        if(last_90.nonEmpty) {
          val firstelement90 = (SDF.parse(list90.head.PAYMENT_DTE).getTime - SDF.parse(last_90.maxBy(t=>SDF.parse(t.PAYMENT_DTE)).PAYMENT_DTE).getTime) / (1000 * 60 * 60 * 24)
          listdiff90=math.abs(firstelement90)::listdiff90
        }

        for (i <- 0 to list90.size - 2) {
          val diff = (SDF.parse(list90(i + 1).PAYMENT_DTE).getTime - SDF.parse(list90(i).PAYMENT_DTE).getTime) / (1000 * 60 * 60 * 24)
          listdiff90= Math.abs(diff) :: listdiff90
        }
        par.delq_paytm_diff_90 = listdiff90.min.toDouble
      }else{
        par.delq_paytm_diff_90= -1.0
      }
      //OK****************************************************************************************
      //计算max_oday_max3（3个月内最大逾期天数）
      val tempmax_3=t._2.filter(t=> (today.getTime-SDF.parse(t.PAYMENT_DTE).getTime)/(1000*60*60*24)<=90
        && t.DELQ_STATUS.contains("1")).map(t=>
        SDF.parse(t.LST_UPD_TIME).getTime-SDF.parse(t.PAYMENT_DTE).getTime
      )
      if(tempmax_3.isEmpty){par.max_oday_max3=0.0}else{
        par.max_oday_max3= (tempmax_3.max/(1000*60*60*24)).toInt.toDouble
      }
      //****************************************************************************************
      //OK计算oday_max1（1个月内最大逾期天数）
      val tempmax_1= t._2.filter(t=> (today.getTime-SDF.parse(t.PAYMENT_DTE).getTime)/(1000*60*60*24)<=30
        && t.DELQ_STATUS.contains("1")).map(t=>
        SDF.parse(t.LST_UPD_TIME).getTime-SDF.parse(t.PAYMENT_DTE).getTime
      )
      if(tempmax_1.isEmpty){par.oday_max1=0.0}else{
        par.oday_max1=(tempmax_1.max/(1000*60*60*24)).toInt.toDouble
      }
      //****************************************************************************************
      //计算timeStamp
      par.timeStamp=t._2.maxBy(t=> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(t.LST_UPD_TIME)).LST_UPD_TIME
      //返回每个用户对应的指标value
      (t._1, par)}
    )).cache()

    val scoreRdd=customParRdd.mapPartitions(itertor=>itertor.map(t=>{
            println(t._1+"-------oday_max1:  "+t._2.oday_max1)
            println(t._1+"-------max_oday_max3:  "+t._2.max_oday_max3)
            println(t._1+"-------delq_paytm_diff_30:  "+t._2.delq_paytm_diff_30)
            println(t._1+"-------delq_paytm_diff_90:  "+t._2.delq_paytm_diff_90)
      //****************************************************************************************
      //计算delq_paytm_diff_30minbox的分数
      if(t._2.delq_paytm_diff_30<0){
        t._2.delq_paytm_diff_30= -0.078426
      }else if(t._2.delq_paytm_diff_30<=1 ){
        t._2.delq_paytm_diff_30= 0.207198
      }else if(t._2.delq_paytm_diff_30<=21 ){
        t._2.delq_paytm_diff_30=0.168158
      } else if(t._2.delq_paytm_diff_30<=33){
        t._2.delq_paytm_diff_30=0.135095
      }else if(t._2.delq_paytm_diff_30<=90){
        t._2.delq_paytm_diff_30=0.075059
      }else {
        t._2.delq_paytm_diff_30=0.004081
      }
      //****************************************************************************************
      //计算delq_paytm_diff_90minbox的分数
      if(t._2.delq_paytm_diff_90<0){
        t._2.delq_paytm_diff_90= -0.113874
      }else if(t._2.delq_paytm_diff_90<=12){
        t._2.delq_paytm_diff_90= 0.152233
      }else if(t._2.delq_paytm_diff_90<=42){
        t._2.delq_paytm_diff_90=0.073718
      }else if(t._2.delq_paytm_diff_90<=120){
        t._2.delq_paytm_diff_90=0.008228
      }else {
        t._2.delq_paytm_diff_90= -0.050411
      }
      //****************************************************************************************
      //计算max_oday_max3box的分数
      if(t._2.max_oday_max3<=0 ){
        t._2.max_oday_max3= -0.485401
      }else if(t._2.max_oday_max3==1) {
        t._2.max_oday_max3= -0.116463
      }else if(t._2.max_oday_max3==2){
        t._2.max_oday_max3=0.459996
      }else{
        t._2.max_oday_max3= 0.880535
      }
      //****************************************************************************************
      //计算oday_max1box的分数
      if(t._2.oday_max1<=0  ){
        t._2.oday_max1= -0.221718
      }else if(t._2.oday_max1==1) {
        t._2.oday_max1= 0.072182
      }else{
        t._2.oday_max1=0.689110
      }

      //****************************************************************************************
      //计算sum打分
      val totalScore1:Double=t._2.oday_max1+t._2.max_oday_max3
      val totalScore2=t._2.delq_paytm_diff_30+t._2.delq_paytm_diff_90
      val totalScore=totalScore1+totalScore2
            println(t._2.oday_max1+t._2.max_oday_max3+t._2.delq_paytm_diff_30+"-------------------------")

            println(t._1+"******oday_max1:  "+t._2.oday_max1)
            println(t._1+"******max_oday_max3:  "+t._2.max_oday_max3)
            println(t._1+"******delq_paytm_diff_30:  "+t._2.delq_paytm_diff_30)
            println(t._1+"******delq_paytm_diff_90:  "+t._2.delq_paytm_diff_90)
            //println("value:"+t._2.latest_oday_max)
            println("sum"+totalScore)
            //计算B_score
      val B_score=500+50/Math.log(2)*(-(totalScore+Intercept))
      (t._1, Double.valueOf(B_score.formatted("%.5f")),t._2.timeStamp)
    }))
    //****************************************************************************************
    //返回（CUST_NUM,B_score,timeStamp）Rdd
    return scoreRdd
  }



}

