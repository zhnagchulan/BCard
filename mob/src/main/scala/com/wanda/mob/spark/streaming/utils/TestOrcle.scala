package com.wanda.mob.spark.streaming.utils

import java.sql.DriverManager
import java.util.Properties

import com.wanda.mob.spark.streaming.main.Offline.conf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object TestOrcle {
  val conf=new SparkConf().set("spark.local.dir", "D:\\shuffle_dir")
  val session: SparkSession = SparkSession.builder().config(conf).appName("2").master("local[*]").getOrCreate()
  val driver = "oracle.jdbc.driver.OracleDriver"
  val url = "jdbc:oracle:thin:@172.21.230.20:1530:carddb"
  val username="wise_query"
  val password="lMwyNmfHTDBs3tQR"
//  val connection = DriverManager.getConnection(url,username,password)
  Class.forName(driver)

  def main(args: Array[String]): Unit = {

//    val sql="select count(1) from WISE.LM_CUSTOMER"
//    val result=connection.createStatement().executeQuery(sql)
//    while(result.next()){
//      println
//    }
//    connection.close()
    val properties=new Properties()
    properties.setProperty("user","wise_query")
    properties.setProperty("password","lMwyNmfHTDBs3tQR")
   session.read.jdbc(url,"WISE.lm_customer",properties).createOrReplaceTempView("lm_customer")
   session.read.jdbc(url,"WISE.fcs_acct_register",properties).createOrReplaceTempView("rs_register")
    //
      val userNanDataFormat = session.sql(
        s"""
           |select distinct d.member_code,d.audit_time,(case when d.cust_nbr is null and datediff(now(),d.audit_time)>=90 then 1
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
        """.stripMargin
      ).show(10)

      //
      //
      //    val register = session.read.format("jdbc")
      //      .option("driver", driver)
      //      .option("url", url)
      //      .option("dbtable", "WISE.LM_CUSTOMER")
      //      .option("user", username)
      //      .option("password", password)
      //        .load().show(10)
      session.close()

    }
}
