//package com.wanda.mob.spark.streaming.test
//
//import com.wanda.mob.spark.streaming.main.Offline._
//import com.wanda.mob.spark.streaming.mysql.MConnector
//import com.wanda.mob.spark.streaming.utils.{DateUtils, Stopwatch}
//import org.apache.kudu.spark.kudu.KuduContext
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SparkSession
//
//object asd {
//
//  val conf: SparkConf = new SparkConf().setAppName("B Creadit Model Daily Calculation")
//
//        val sc = new SparkContext("local[*]", "test111", conf)
//    conf.set("spark.local.dir", "D:\\shuffle_dir")
////  val sc = new SparkContext(conf)
//
//  sc.setLogLevel("WARN")
//
//  val session: SparkSession = SparkSession.builder().getOrCreate()
//  private val _KUDU_MASTER = "172.21.152.21,172.21.152.22,172.21.152.23"
//
//  /**
//    * B卡模型
//    */
//
//  private val _B_CM_BOX: String = "impala::default.b_cm_box"
//
//  /**
//    * 交易主表
//    */
//
//  private val _LM_INSTALLMENT_TRAN: String = "impala::default.LM_INSTALLMENT_TRAN"
//  private val _TS_INSTALLMENT_MAIN: String = "impala::default.TS_INSTALLMENT_MAIN"
//
//
//  /**
//    * 交易明细
//    */
//
//  private val _LM_INSTALLMENT_TRAN_D: String = "impala::default.LM_INSTALLMENT_TRAN_D"
//  private val _TS_INSTALLMENT_DETAIL: String = "impala::default.TS_INSTALLMENT_DETAIL"
//
//  /**
//    * 注册表
//    */
//
//  private val _FCS_ACCT_REGISTER: String = "impala::default.FCS_ACCT_REGISTER"
//
//  /**
//    * 鹏元学历
//    */
//
//  private val _T_CTM_KYH_PBOC_REQ: String = "impala::default.t_ctm_kyh_pboc_req"
//
//  /**
//    * 通讯录
//    */
//
//  private val _ORIGINAL_USER_CONTACTS: String = "impala::default.original_user_contacts"
//
//  /**
//    * 过滤所需表
//    */
//
//  private val _LM_CUSTOMER: String = "impala::default.LM_CUSTOMER"
//
//  /**
//    * mysql相关信息
//    */
//  private val mysql_url = "jdbc:mysql://172.21.230.18:3306/crd?useUnicode=true&characterEncoding=utf8"
//  private val driver = "com.mysql.jdbc.Driver"
//  private val username = "crduser"
//  private val password = "MZTWhdJdH0Ypvnx1"
//
//  private val mysql_table_d = "b_cm_box"
//  private val mysql_table_m = "b_cm_box_m"
//
//  def main(args: Array[String]): Unit = {
//
//    val totalProcessingTime = new Stopwatch()
//
//    val kc = new KuduContext(_KUDU_MASTER, sc)
//
//    //    val b_ol_grade: Map[String, String] = Map(
//    //      "kudu.table" -> _B_CREDIT_OL_GRADE,
//    //      "kudu.master" -> _KUDU_MASTER)
//    val tran: Map[String, String] = Map(
//      "kudu.table" -> _LM_INSTALLMENT_TRAN,
//      "kudu.master" -> _KUDU_MASTER)
//
//    val main: Map[String, String] = Map(
//      "kudu.table" -> _TS_INSTALLMENT_MAIN,
//      "kudu.master" -> _KUDU_MASTER)
//
//    val d: Map[String, String] = Map(
//      "kudu.table" -> _LM_INSTALLMENT_TRAN_D,
//      "kudu.master" -> _KUDU_MASTER)
//
//    val detail: Map[String, String] = Map(
//      "kudu.table" -> _TS_INSTALLMENT_DETAIL,
//      "kudu.master" -> _KUDU_MASTER)
//
//    val reg: Map[String, String] = Map(
//      "kudu.table" -> _FCS_ACCT_REGISTER,
//      "kudu.master" -> _KUDU_MASTER)
//
//    val degree: Map[String, String] = Map(
//      "kudu.table" -> _T_CTM_KYH_PBOC_REQ,
//      "kudu.master" -> _KUDU_MASTER)
//
//    val contacts: Map[String, String] = Map(
//      "kudu.table" -> _ORIGINAL_USER_CONTACTS,
//      "kudu.master" -> _KUDU_MASTER)
//
//    val customer: Map[String, String] = Map(
//      "kudu.table" -> _LM_CUSTOMER,
//      "kudu.master" -> _KUDU_MASTER)
//
//    val b_box: Map[String, String] = Map(
//      "kudu.table" -> _B_CM_BOX,
//      "kudu.master" -> _KUDU_MASTER)
//
//    val globalDate = sc.broadcast(DateUtils.getCurrentDate)
//
// new MConnector().clearTwoDaysAgo(
//   mysql_table_d,mysql_url,driver,username,
//   password,globalDate
// )
//  }
//
//}
