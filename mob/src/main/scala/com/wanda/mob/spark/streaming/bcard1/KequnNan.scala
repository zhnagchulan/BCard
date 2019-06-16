package com.wanda.mob.spark.streaming.bcard1

import java.util.Properties

import com.wanda.mob.spark.streaming.main.New_Offline.session

object KequnNan {

  val url = "jdbc:oracle:thin:@172.21.230.20:1530:carddb"
  val username="wise_query"
  val password="lMwyNmfHTDBs3tQR"
  val driver = "oracle.jdbc.driver.OracleDriver"
  Class.forName(driver)

  val properties=new Properties()
  properties.setProperty("user","wise_query")
  properties.setProperty("password","lMwyNmfHTDBs3tQR")
  session.read.jdbc(url,"WISE.lm_customer",properties).createOrReplaceTempView("lm_customer")
  session.read.jdbc(url,"WISE.fcs_acct_register",properties).createOrReplaceTempView("rs_register")
  def kequn_RDD9() = {
   lazy val userNanDataFormat = session.sql(
     s"""
        |select distinct d.member_code,(case when d.cust_nbr is null and datediff(now(),d.audit_time)>=90 then 1
        |when datediff(now(),d.audit_time)<90 then 9 else 0 end) as groups
        |from(
        |select a.member_code,a.cust_nbr,a.status,max(a.audit_time) as audit_time
        |from(
        |select register.member_code,register.cust_nbr,register.audit_time,customer.status
        |from rs_register as register
        |left join lm_customer as customer
        |on register.member_code=customer.external_id
        |)a
        |where a.status is null or a.status='0'
        |group by a.member_code,a.cust_nbr,a.status
        |)d
        |having groups!=0
        """.stripMargin
    )

    val kequn_9=userNanDataFormat.rdd.map(x=>{
      (x.get(0).toString,null:String,x.get(1).toString,null:String)
    })
    kequn_9.foreach(println)
    kequn_9
  }

  def main(args: Array[String]): Unit = {
    kequn_RDD9()

  }
}
