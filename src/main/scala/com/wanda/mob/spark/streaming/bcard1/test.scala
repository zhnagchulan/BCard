package com.wanda.mob.spark.streaming.bcard1

import com.wanda.mob.spark.streaming.models.Model_Four
import com.wanda.mob.spark.streaming.utils.CustomClassing

object test {
    def main(args: Array[String]): Unit = {
         //测试获得逾期的客户
      // println("base:"+CustomClassing.baseRdd.map(t=>t._1).distinct().count())
      //         println("逾期用户："+CustomClassing.getYqCustomRdd().toJavaRDD().collect())
      //         //测试非逾期客户
      //          println("非逾期用户："+CustomClassing.getNoYqCustomRdd().toJavaRDD().collect())
      //         //测试当前无余额用户
      //         println("当前非逾期无余额用户："+CustomClassing.getNoBankBlanceCustomRdd().toJavaRDD().collect())
      //         //测试当前有余额用户
//          println("当前非逾期有余额用户："+CustomClassing.getBankBlanceCustomRdd().toJavaRDD().collect())
//          //当前逾期有余额历史无逾期用户
//          println("当前非逾期有余额历史无逾期用户："+CustomClassing.getHistoryNoYqCustomRDD().toJavaRDD().collect())
//         //当前逾期有余额历史无逾期用户
//         println("当前非逾期有余额历史无逾期无还款用户："+CustomClassing.getHistoryNoRepaymentCustomRdd().toJavaRDD().collect())
//         //当前逾期有余额历史无逾期用户
//         println("当前非逾期有余额历史无逾期有还款用户："+CustomClassing.getHistoryRepaymentCustomRdd().toJavaRDD().collect())
//         //当前逾期有余额历史有逾期[1,3]用户
//          println("当前非逾期有余额历史有逾期[1,3]天用户："+CustomClassing.getMaxYqOneToThreeCustomRdd().toJavaRDD().collect())
//          //当前逾期有余额历史有逾期[4,)用户
//          println("当前非逾期有余额历史有逾期[4,)天用户："+CustomClassing.getMaxYqGreatFourCustomRdd().toJavaRDD().collect())

      //
      //
//        //测试模型4
//          println("客户群7，模型4打分"+
//            Model_Four.scoring(GetGroupedCustomAllData.getMaxYqGreatFourCustomDataRdd()).toJavaRDD().collect())
      //
          //测试客户分群8
//          println("客户群8，不计入模型"+
//            GetFinalGroupingResults.getGroupEightResults().toJavaRDD().collect())


   //println("所有客户"+
     // GetFinalGroupingResults.resultsUnion().toJavaRDD().collect())



    }




  }