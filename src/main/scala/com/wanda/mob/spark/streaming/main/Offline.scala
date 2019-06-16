package com.wanda.mob.spark.streaming.main

import com.wanda.mob.spark.streaming.accumulator.factory.AccumulatorFactory
import com.wanda.mob.spark.streaming.bcard1.GetFinalGroupingResults
import com.wanda.mob.spark.streaming.event.impl.{CommonEvent, ScoreEvent}
import com.wanda.mob.spark.streaming.kafka.KafkaHelper
import com.wanda.mob.spark.streaming.kafka.offsetsv1.MyKafkaUtils
import com.wanda.mob.spark.streaming.kudu.factory.DAOFactory
import com.wanda.mob.spark.streaming.utils._
import org.I0Itec.zkclient.ZkClient
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.HasOffsetRanges

object Offline {


  val conf: SparkConf = new SparkConf().setAppName("B Creadit Model Daily Calculation")

  val sc = new SparkContext("local[4]", "test111", conf)
  conf.set("spark.local.dir", "D:\\shuffle_dir")
//val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

  val session: SparkSession = SparkSession.builder().getOrCreate()
  private val _KUDU_MASTER = "172.21.152.21,172.21.152.22,172.21.152.23"

  /**
    * B卡模型
    */

  private val _B_CM_BOX: String = "impala::default.b_cm_box"

  /**
    * 交易主表
    */

  private val _LM_INSTALLMENT_TRAN: String = "impala::default.LM_INSTALLMENT_TRAN"
  private val _TS_INSTALLMENT_MAIN: String = "impala::default.TS_INSTALLMENT_MAIN"


  /**
    * 交易明细
    */

  private val _LM_INSTALLMENT_TRAN_D: String = "impala::default.LM_INSTALLMENT_TRAN_D"
  private val _TS_INSTALLMENT_DETAIL: String = "impala::default.TS_INSTALLMENT_DETAIL"

  /**
    * 注册表
    */

  private val _FCS_ACCT_REGISTER: String = "impala::default.FCS_ACCT_REGISTER"

  /**
    * 鹏元学历
    */

  private val _T_CTM_KYH_PBOC_REQ: String = "impala::default.t_ctm_kyh_pboc_req"

  /**
    * 通讯录
    */

  private val _ORIGINAL_USER_CONTACTS: String = "impala::default.original_user_contacts"

  /**
    * 过滤所需表
    */

  private val _LM_CUSTOMER: String = "impala::default.LM_CUSTOMER"

  /**
    * mysql相关信息
    */
  private val mysql_url = "jdbc:mysql://172.21.230.18:3306/test?useUnicode=true&characterEncoding=utf8"
  private val driver = "com.mysql.jdbc.Driver"
  private val username = "bcardmod"
  private val password = "GGCv5%#@XSE6Yh3K"

  private val t1_table = "test_b_t1_box"
  private val m_table = "test_b_m_box"

  def main(args: Array[String]): Unit = {


    val totalProcessingTime = new Stopwatch()

    val kc = new KuduContext(_KUDU_MASTER, sc)

    //    val b_ol_grade: Map[String, String] = Map(
    //      "kudu.table" -> _B_CREDIT_OL_GRADE,
    //      "kudu.master" -> _KUDU_MASTER)
    val tran: Map[String, String] = Map(
      "kudu.table" -> _LM_INSTALLMENT_TRAN,
      "kudu.master" -> _KUDU_MASTER)

    val main: Map[String, String] = Map(
      "kudu.table" -> _TS_INSTALLMENT_MAIN,
      "kudu.master" -> _KUDU_MASTER)

    val d: Map[String, String] = Map(
      "kudu.table" -> _LM_INSTALLMENT_TRAN_D,
      "kudu.master" -> _KUDU_MASTER)

    val detail: Map[String, String] = Map(
      "kudu.table" -> _TS_INSTALLMENT_DETAIL,
      "kudu.master" -> _KUDU_MASTER)

    val reg: Map[String, String] = Map(
      "kudu.table" -> _FCS_ACCT_REGISTER,
      "kudu.master" -> _KUDU_MASTER)

    val degree: Map[String, String] = Map(
      "kudu.table" -> _T_CTM_KYH_PBOC_REQ,
      "kudu.master" -> _KUDU_MASTER)

    val contacts: Map[String, String] = Map(
      "kudu.table" -> _ORIGINAL_USER_CONTACTS,
      "kudu.master" -> _KUDU_MASTER)

    val customer: Map[String, String] = Map(
      "kudu.table" -> _LM_CUSTOMER,
      "kudu.master" -> _KUDU_MASTER)

    val b_box: Map[String, String] = Map(
      "kudu.table" -> _B_CM_BOX,
      "kudu.master" -> _KUDU_MASTER)

        val globalDate = sc.broadcast(DateUtils.getCurrentDate)

    //合并交易表并过滤掉所有指标的通用规则
    val commonEventRDD = DAOFactory.getKuduInteractionLayer.doExtractBySQL(
      (lm_tran, ts_main, lm_d, ts_detail, customer, reg) => {
        s"""select filtered_tran.*,reg.cash_amt,reg.audit_time from (
           |select tran.* from (
           |select main.*,
           |d.payment_dte,d.lst_upd_time,d.delq_status,d.pymt_flag,d.tran_amt_paid
           |from (
           |select * from(
           |select cust_nbr,account_nmbr,posting_dte,principal,status,transaction_type
           |from $lm_tran where status != "5"
           |union all
           |select cust_nbr,cast(id as string) as account_nmbr,txn_time as posting_dte,tran_amt as principal,cast (status as string),null
           |from $ts_main where status in (0,1,4) and txn_type=1
           |) dd where dd.cust_nbr like '1200000010008805446'
           |) main
           |INNER JOIN(
           |select account_nmbr,payment_dte,lst_upd_time,delq_status,pymt_flag,tran_amt_paid
           |from $lm_d where TRAN_TYPE='4000' and pymt_flag in ('00','01')
           |UNION ALl
           |select cast(INSTALLMENT_ID as String) as account_nmbr,PAYMENT_DATE as payment_dte,
           |case when repay_time is null then from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')
           |else repay_time end lst_upd_time,
           |cast(overdue_flag as string) as delq_status,cast(status as string) as pymt_flag,tran_amt_paid
           |from $ts_detail where status in (0,1,2,4)
           |) d
           |on main.account_nmbr = d.account_nmbr
           |
          |) tran
           |
          |inner join (select cust_nbr from $customer where status = '0') customer on customer.cust_nbr = tran.cust_nbr
           |) filtered_tran
           |
          |inner join (select cust_nbr,cash_amt,audit_time
           |from $reg where datediff(to_date(now()),to_date(reg_time)) >= 90) reg on filtered_tran.cust_nbr = reg.cust_nbr
           |""".stripMargin
      }, tran, main, d, detail, customer, reg, session
    )
      .rdd
      .map(row => KuduUtils.convertSQLRowIntoEvent(row, CommonEvent()))


//    import session.implicits._
    //调用打分模型
    //        传入session,commonEventRDD等参数
    val RDDRow = GetFinalGroupingResults.resultsUnion(commonEventRDD,globalDate)
      .map(e=>Converter.convertEventIntoRow(e))
      .foreach(x => {
//        println((x.CUST_NBR, x.DATE_D, x.GROUPS, x.SCORE, x.LST_UPD_TIME))
      })


//        DAOFactory.getKuduInteractionLayer.doUpsertByDefaultAPI(RDDRow,ScoreEvent(),_B_CM_BOX
//          ,session,kc)




    println("耗时" + totalProcessingTime)
  }
}