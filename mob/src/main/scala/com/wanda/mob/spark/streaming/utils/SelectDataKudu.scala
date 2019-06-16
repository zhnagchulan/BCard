package com.wanda.mob.spark.streaming.utils

import com.wanda.mob.spark.streaming.kudu.factory.DAOFactory
import org.apache.spark.sql.SparkSession
import org.apache.kudu.spark.kudu._
import org.apache.spark.SparkConf
//import com.wanda.mob.spark.streaming.main.Offline.session

object SelectDataKudu {
  val conf=new SparkConf().set("spark.local.dir", "D:\\shuffle_dir")
    val session = SparkSession.builder().appName("kudu").master("local[*]").config(conf).getOrCreate()
  def tran_dd() = {
    val _KUDU_MASTER = "172.21.152.21,172.21.152.22,172.21.152.23"
    val _LM_INSTALLMENT_TRAN: String = "impala::default.LM_INSTALLMENT_TRAN"
    val _TS_INSTALLMENT_MAIN: String = "impala::default.TS_INSTALLMENT_MAIN"
    val _LM_INSTALLMENT_TRAN_D: String = "impala::default.LM_INSTALLMENT_TRAN_D"
    val _TS_INSTALLMENT_DETAIL: String = "impala::default.TS_INSTALLMENT_DETAIL"
    val _FCS_ACCT_REGISTER: String = "impala::default.FCS_ACCT_REGISTER"
    val _LM_CUSTOMER: String = "impala::default.LM_CUSTOMER"


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

    val customer: Map[String, String] = Map(
      "kudu.table" -> _LM_CUSTOMER,
      "kudu.master" -> _KUDU_MASTER)


    val commonEventRDD = DAOFactory.getKuduInteractionLayer.doExtractBySQL(
      (lm_tran, ts_main, lm_d, ts_detail, customer, reg) => {
        s"""
           |select distinct a.* from $customer a
           |INNER JOIN(
           |select customer.cust_nbr,
           |tran.account_nmbr,tran.posting_dte,tran.principal,tran.status,tran.transaction_type,
           |tran.payment_dte,tran.lst_upd_time,tran.delq_status,tran.pymt_flag,tran.tran_amt_paid,
           |reg.cash_amt,reg.audit_time from(
           |select main.*,
           |d.payment_dte,d.lst_upd_time,d.delq_status,d.pymt_flag,d.tran_amt_paid
           |from (
           |select cust_nbr,account_nmbr,posting_dte,principal,status,transaction_type
           |from $lm_tran where status != "5"
           |union all
           |select cust_nbr,cast(id as string) as account_nmbr,txn_time as posting_dte,tran_amt as principal,cast (status as string),null
           |from $ts_main where status in (0,1,4) and txn_type=1
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
          |right join (select cust_nbr from $customer where status = '0') customer on customer.cust_nbr = tran.cust_nbr
           |
          |left join (
           |select t.cust_nbr,t.cash_amt,t.audit_time,t.member_code
           |from $reg t inner join (
           |select max(crt_time) as crt_time,cust_nbr from $reg
           |group by cust_nbr) x
           |on x.cust_nbr=t.cust_nbr and x.crt_time=t.crt_time
           |) reg on customer.cust_nbr = reg.cust_nbr
           |having customer.cust_nbr like '12000000100088%')b
           |on a.cust_nbr=b.cust_nbr
        """.stripMargin
      }, tran, main, d, detail, customer, reg, session
    )

//    tran_dppp.show()
    commonEventRDD.repartition(1).write.csv("D:\\Alex\\kudu\\customer")
//    val ss=commonEventRDD.rdd.count()
//    val ss=commonEventRDD.rdd.map(x=>x.get(5).toString).distinct().count()
//    println(ss+"**************************")
  }

  def main(args: Array[String]): Unit = {
    tran_dd()
    println("Yes!!!!!!!!!!1")
  }

}
