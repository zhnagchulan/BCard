package com.wanda.mob.spark.streaming.main

import com.wanda.mob.spark.streaming.accumulator.factory.AccumulatorFactory
import com.wanda.mob.spark.streaming.bcard1.{GetFinalGroupingResults, GetGroupedCustomAllData}
import com.wanda.mob.spark.streaming.event.Event
import com.wanda.mob.spark.streaming.event.impl.CommonEvent
import com.wanda.mob.spark.streaming.kafka.KafkaHelper
import com.wanda.mob.spark.streaming.kafka.offsetsv1.MyKafkaUtils
import com.wanda.mob.spark.streaming.kudu.factory.DAOFactory
import com.wanda.mob.spark.streaming.test.TestFact
import com.wanda.mob.spark.streaming.utils._
import org.I0Itec.zkclient.ZkClient
import org.apache.kudu.client.{KuduException, KuduPredicate, KuduScanner}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

object Streaming {
  val conf = new SparkConf()
    .setAppName("Spark On Kudu")
    .set("spark.streaming.kafka.maxRatePerPartition", "5000")
    .set("spark.streaming.stopGracefullyOnShutdown", "true")
    .set("spark.streaming.backpressure.enabled", "true")
  conf.set("spark.local.dir", "D:\\shuffle_dir")

  //        val sc = new SparkContext(conf)
  val sc = new SparkContext("local[*]", "test111", conf)

  sc.setLogLevel("warn")
  val session = SparkSession.builder().getOrCreate()

  private val _KUDU_MASTER = "172.21.152.21,172.21.152.22,172.21.152.23"

  /**
    * B卡模型
    */

  private val _B_CMS_BOX: String = "impala::default.b_cms_box"

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


  private val _ZK_HOSTS: String = "172.21.152.21:2181,172.21.152.22:2181,172.21.152.23:2181"
  private val _BROKER_LIST: String = "172.21.152.21:9092,172.21.152.22:9092,172.21.152.23:9092"
  private val _consumerGroup: String = "b-cm-rt-group"

  private val _topicList: String = "b_cm_rt_custs"

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
    "kudu.table" -> _B_CMS_BOX,
    "kudu.master" -> _KUDU_MASTER)

  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//      .setAppName("Spark On Kudu")
//      .set("spark.streaming.kafka.maxRatePerPartition", "5000")
//      .set("spark.streaming.stopGracefullyOnShutdown", "true")
//      .set("spark.streaming.backpressure.enabled", "true")
//    conf.set("spark.local.dir", "D:\\shuffle_dir")
//
//    //        val sc = new SparkContext(conf)
//    val sc = new SparkContext("local[*]", "test111", conf)
//
//    sc.setLogLevel("warn")

//    val session = SparkSession.builder().getOrCreate()


    val ssc = new StreamingContext(sc, Seconds(30))
    val kuduContext = new KuduContext(_KUDU_MASTER, sc)

    val kafkaParams = Map[String, String]("metadata.broker.list" -> _BROKER_LIST)
    val zkClient = new ZkClient(_ZK_HOSTS)

    //Todo cheackPoint元数据
    //    val messagesDStream = MyKafkaUtils.createKafkaStream(
    //      ssc,
    //      kafkaParams,
    //      zkClient,
    //      _consumerGroup,
    //      _topicList,
    //      _BROKER_LIST).cache()

    val messagesDStream = KafkaHelper.loadTopicAndMessageFromKafka(ssc, _topicList, kafkaParams)

    processInputDStream(
      messagesDStream,
      sc,
      session,
      kuduContext,
      _KUDU_MASTER)

    messagesDStream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      MyKafkaUtils.saveUntilOffsets(zkClient, _consumerGroup, _topicList, offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()

  }


  def processInputDStream(messages: DStream[(String, String)],
                          sc: SparkContext,
                          session: SparkSession,
                          kc: KuduContext,
                          kuduMaster: String): Unit = {
    //kafka肯定有重复数据，累加器set会去重
    val cusAccum = new AccumulatorFactory[String]().getHashSetAccInstance(sc)
    val accAccum = new AccumulatorFactory[String]().getHashSetAccInstance(sc)

    val ieDStream = messages.mapPartitions(
      iter => {
        for (topicAndJson <- iter) yield {
          ParseUtils.parseJsonToRTInputEvent(topicAndJson._2) match {
            case Some(e) => cusAccum.add(e.CUST_NBR)
            case None =>

          }
        }
      }
    )
      .cache()

    ieDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {

        val tranRDD = DAOFactory.getKuduInteractionLayer.doExtractBySQL(
          (tran, main, customer, reg) => {
            s"""select * from (
               |
          |select main.* from (
               |select cust_nbr,account_nmbr,posting_dte,principal,status,transaction_type
               |from $tran where status != "5"
               |and cust_nbr in (${ParseUtils.getCustNumsFromSet(cusAccum.value)})
               |union all
               |select cust_nbr,cast(id as string) as account_nmbr,txn_time as posting_dte,tran_amt as principal,
               |cast (status as string),null from $main where status in (0,1,4) and txn_type=1
               |and cust_nbr in (${ParseUtils.getCustNumsFromSet(cusAccum.value)})
               |) main
               |
          |inner join (select cust_nbr from $customer where cust_nbr in (${ParseUtils.getCustNumsFromSet(cusAccum.value)})
               |and status = '0') customer on customer.cust_nbr = main.cust_nbr
               |) filtered_tran
               |
          |inner join (select cust_nbr,member_code,cash_amt,audit_time
               |from $reg where cust_nbr in (${ParseUtils.getCustNumsFromSet(cusAccum.value)})
               |and datediff(to_date(now()),to_date(reg_time)) >= 90) reg on filtered_tran.cust_nbr = reg.cust_nbr
               |""".stripMargin
          }, tran, main, customer, reg, session
        )
          .rdd
          .map(row => {
            accAccum.add(row.getString(1))
            (row.getString(1), row)
          })
          .cache()

        tranRDD.foreach(a => a)

        //        println("accccccccc总共="+accAccum.getTotalNbr)

        val tranDRDD = DAOFactory.getKuduInteractionLayer.doExtractBySQL(
          (d, detail) => {
            s"""select ACCOUNT_NMBR,payment_dte,lst_upd_time,delq_status,pymt_flag,tran_amt_paid
               |from $d where account_nmbr in (${ParseUtils.getCustNumsFromSet(accAccum.value)})
               |and TRAN_TYPE='4000' and pymt_flag in ('00','01')
               |UNION ALl
               |select cast(INSTALLMENT_ID as String) as ACCOUNT_NMBR,PAYMENT_DATE as PAYMENT_DTE,
               |case when repay_time is null then from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')
               |else repay_time end lst_upd_time,
               |cast(overdue_flag as string) as delq_status,cast(status as string) as pymt_flag,tran_amt_paid
               |from $detail where INSTALLMENT_ID in (${ParseUtils.getCustNumsFromSet(accAccum.value)})
               |and status in (0,1,2,4)
               |""".stripMargin
          }, d, detail, session
        )

          .rdd
          .map(row => {
            (row.getString(0), row)
          })

        val commonEventRDD = tranRDD
          .join(tranDRDD)
          .mapPartitions(iter => for (tuple <- iter) yield KuduUtils.convertSQLRowIntoEvent(tuple._2._1, tuple._2._2, CommonEvent()))

        commonEventRDD.foreach(e => {
            println("event=" +
              e.CUST_NBR,
              e.ACCOUNT_NMBR,
              e.PYMT_FLAG,
              e.DELQ_STATUS,
              e.PRINCIPAL,
              e.TRAN_AMT_PAID,
              e.CASH_AMT,
              e.POSTING_DTE,
              e.STATUS,
              e.TRANSACTION_TYPE,
              e.PAYMENT_DTE,
              e.LST_UPD_TIME,
              e.AUDIT_TIME)
          })

        //调用打分模型
        //        传入session,commonEventRDD等参数
//        GetFinalGroupingResults.resultsUnion(commonEventRDD)
//            .foreach(x=>{
//              println((x.CUST_NBR,x.DATE_D,x.GROUP,x.SCORE,x.LST_UPD_TIME))
//            })
//          .foreach(x=>{
//            (x.CUST_NBR,x.DATE_D,x.GROUP,x.SCORE,x.LST_UPD_TIME)
//          })


        accAccum.reset()
        cusAccum.reset()

      }
    })

  }
}
