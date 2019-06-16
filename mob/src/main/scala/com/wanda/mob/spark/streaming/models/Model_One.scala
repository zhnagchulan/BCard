package com.wanda.mob.spark.streaming.models

import java.text.SimpleDateFormat
import java.lang.Double

import com.wanda.mob.spark.streaming.event.impl.CommonEvent
import org.apache.spark.rdd.RDD
class Model_One() {

}
object Model_One {


  /** *
    * 求出实动率以及transaction_type=3的模型1参数posting_audit 单位:小时
    *
    * @return
    */
  def getHH(ss: RDD[Tuple2[String, Iterable[CommonEvent]]]): RDD[Tuple4[String, String, String, String]] = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:ss:mm")
    val rdd = ss.flatMap(x => {
      for (i <- x._2) yield {
        i
      }
    }).mapPartitions(item =>
      for (x <- item) yield {
//        println(x.CUST_NBR+"88888"+x.PRINCIPAL)
        (x.MEMBER_CODE,x.ACCOUNT_NMBR,x.PYMT_FLAG,x.DELQ_STATUS,Double.valueOf(x.PRINCIPAL),Double.valueOf(x.TRAN_AMT_PAID),
        Double.valueOf(x.CASH_AMT),x.POSTING_DTE,x.STATUS,x.TRANSACTION_TYPE,x.PAYMENT_DTE,x.LST_UPD_TIME,x.AUDIT_TIME)
      }
    )
    val shidong_rate = rdd.mapPartitions(item => {
      for (x <- item) yield {
        ((x._1, x._2), (x._5, x._6, x._7))
      }
    })
      .reduceByKey((x, y) => {
        (x._1, x._2 + y._2, x._3) //根据 x.cust_nbr,x.account_nmbr去重 principal 第一次聚合tran_amt_pai
      })
      .mapPartitions(item => {
        for (x <- item) yield {
          (x._1._1, (x._2._1, x._2._2, x._2._3))
        }
      })
      .reduceByKey((x, y) => {
        (x._1 + y._1, x._2 + y._2, x._3) //根据x.cust_nbr 第一次聚合principal 第二次聚合tran_amt_pai
      })mapPartitions(item => {
        for (x <- item) yield {
          (x._1,(x._2._1 -x._2._2))
          (x._1, ((x._2._1 - x._2._2) / x._2._3).formatted("%.5f").toDouble) //求出实动率

        }
      })
//    shidong_rate.foreach(x=>println(x._1,x._2,": --99999"))
    val posting_audit = rdd.map(x => {
      (x._1, (x._8, x._10, x._13, x._12))
    }
    ).reduceByKey((x, y) => {
        (if (x._1 < y._1) x._1 else y._1, x._2, x._3, if (x._4 > y._4) x._4 else y._4)
      }).mapPartitions(item => {
      for (x <- item) yield {
        (x._1, (
          (simpleDateFormat.parse(x._2._1).getTime - simpleDateFormat.parse(x._2._3).getTime) / (1000 * 60 * 60)
          , x._2._4)
        )
      }
    })
    val modelOne = shidong_rate.join(posting_audit)
      //modelOne.foreach(println)
    val score = modelOne
      .mapPartitions(item => {
      for (x <- item) yield {
        if (x._2._1 >= 1.0) {
          if (x._2._2._1 >= 24.0) {
            (x._1, 0.0556, 4, x._2._2._2) //If shidong_rate >=1 and posting_audit >=24 then score=0.0556
          } else {
            (x._1, 0.1204, 4, x._2._2._2) //If shidong_rate >=1 and posting_audit <24 then score= 0.1204
          }
        } else {
          (x._1, 0.0173, 4, x._2._2._2) //If shidong_rate <1 then score = 0.0173
        }
      }
    })
    score.mapPartitions(item =>
      for (x <- item) yield {
          (x._1,(500 + (50 / Math.log(2)) * Math.log(1 / x._2 - 1)).formatted("%.5f")
            , x._3.toString, x._4)
      }
    )
  }

}

