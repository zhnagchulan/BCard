package com.wanda.mob.spark.streaming.main

import java.util.Properties

import com.wanda.mob.spark.streaming.bcard1.GetFinalGroupingResults
import com.wanda.mob.spark.streaming.event.impl.CommonEvent
import com.wanda.mob.spark.streaming.mysql.MConnector
import com.wanda.mob.spark.streaming.utils.{Converter, DateUtils, KuduUtils, Stopwatch}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object New_Offline {

  val conf: SparkConf = new SparkConf().setAppName("B Creadit Model Daily Calculation")



//      val conf=new SparkConf().set("spark.local.dir", "D:\\shuffle_dir")
//            .appName("MLGB").master("local[*]")
  val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sc: SparkContext = session.sparkContext
  sc.setLogLevel("WARN")


  /**
    * mysql相关信息
    */
//  private val mysql_url = "jdbc:mysql://172.21.230.18:3306/crd?useUnicode=true&characterEncoding=utf8"
  private val mysql_url = "jdbc:mysql://172.21.230.18:3306/bcardmod?useUnicode=true&characterEncoding=utf8"

  private val driver = "com.mysql.jdbc.Driver"
//  private val username = "crduser"
//  private val password = "MZTWhdJdH0Ypvnx1"
  private val username = "bcardmod"
  private val password = "GGCv5%#@XSE6Yh3K"

//  private val upsertMysqlURL= "jdbc:mysql://172.21.230.18:3306/crd?user=crduser&password=MZTWhdJdH0Ypvnx1"
private val upsertMysqlURL= "jdbc:mysql://172.21.230.18:3306/bcardmod?user=bcardmod&password=GGCv5%25%23@XSE6Yh3K"


  private val mysql_table_d = "b_mm_box"
  private val mysql_table_m = "b_mm_box_m"


  /**
    * oracle 相关信息
    */
  private val oracle_url="jdbc:oracle:thin:@172.21.230.20:1530:carddb"
  private val oracle_driver="oracle.jdbc.driver.OracleDriver"
  private val oracle_username="wise_query"
  private val oracle_password="lMwyNmfHTDBs3tQR"
  Class.forName(oracle_driver)
  private val properties=new Properties()

  def main(args: Array[String]): Unit = {
    val totalProcessingTime = new Stopwatch()
    properties.setProperty("user",oracle_username)
    properties.setProperty("password",oracle_password)

    session.read.jdbc(oracle_url,"WISE.lm_installment_tran_d",properties).createOrReplaceTempView("lm_d")
    session.read.jdbc(oracle_url,"WISE.lm_installment_tran",properties).createOrReplaceTempView("lm_tran")
    session.read.jdbc(oracle_url,"WISE.TS_INSTALLMENT_MAIN",properties).createOrReplaceTempView("ts_main")
    session.read.jdbc(oracle_url,"WISE.ts_installment_detail",properties).createOrReplaceTempView("ts_detail")
    session.read.jdbc(oracle_url,"WISE.lm_customer",properties).createOrReplaceTempView("customer")
    session.read.jdbc(oracle_url,"WISE.fcs_acct_register",properties).createOrReplaceTempView("reg")


    val globalDate = sc.broadcast(DateUtils.getCurrentDate)

    //合并交易表并过滤掉所有指标的通用规则
    val commonEventRDD = session.sql(
        s"""select customer.member_code,
           |tran.account_nmbr,tran.posting_dte,tran.principal,tran.status,tran.transaction_type,
           |tran.payment_dte,tran.lst_upd_time,tran.delq_status,tran.pymt_flag,tran.tran_amt_paid,
           |reg.cash_amt,reg.audit_time from(
           |select main.*,
           |d.payment_dte,d.lst_upd_time,d.delq_status,d.pymt_flag,d.tran_amt_paid
           |
           |
           |from (
           |select cust_nbr,account_nmbr,posting_dte,principal,status,transaction_type
           |from lm_tran where status != "5"
           |union all
           |select cust_nbr,cast(id as string) as account_nmbr,txn_time as posting_dte,tran_amt as principal,cast (status as string),null
           |from ts_main where status in (0,1,4) and txn_type=1
           |) main
           |
           |
           |INNER JOIN(
           |select account_nmbr,payment_dte,lst_upd_time,delq_status,pymt_flag,tran_amt_paid
           |from lm_d where TRAN_TYPE='4000' and pymt_flag in ('00','01')
           |UNION ALl
           |select cast(INSTALLMENT_ID as String) as account_nmbr,PAYMENT_DATE as payment_dte,
           |case when repay_time is null then from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')
           |else repay_time end lst_upd_time,
           |cast(overdue_flag as string) as delq_status,cast(status as string) as pymt_flag,tran_amt_paid
           |from ts_detail where status in (0,1,2,4)
           |) d
           |on main.account_nmbr = d.account_nmbr
           |
          |) tran
           |
          |right join (select cust_nbr,external_id as member_code from customer where status = '0') customer on customer.cust_nbr = tran.cust_nbr
           |
           |left join (
           |select t.cust_nbr,t.cash_amt,t.audit_time
           |from reg t
           |inner join (
           |select max(crt_time) as crt_time,cust_nbr from reg
           |group by cust_nbr) x
           |on x.cust_nbr=t.cust_nbr and x.crt_time=t.crt_time
           |) reg on customer.cust_nbr = reg.cust_nbr
        """.stripMargin
    )
      .rdd
      .map(row => KuduUtils.convertSQLRowIntoEvent(row, CommonEvent()))


    val RDD =
      GetFinalGroupingResults.resultsUnion(commonEventRDD, globalDate)
        .cache()

    val RDDRow = RDD.map(e => Converter.convertEventIntoRow(e))
      .cache()
//        RDDRow.collect().foreach(a=>{println("per row=>"+a.toString())})



        new MConnector().upsertToTableDOrM(RDD, upsertMysqlURL, globalDate, mysql_url, driver, username,
      password, mysql_table_d, mysql_table_m)



    println("耗时" + totalProcessingTime)
  }
}
