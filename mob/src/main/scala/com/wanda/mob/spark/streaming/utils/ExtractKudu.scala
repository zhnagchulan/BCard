package com.wanda.mob.spark.streaming.utils
import org.apache.kudu.spark.kudu._
import com.wanda.mob.spark.streaming.main.Streaming.session
import org.apache.spark.sql.SparkSession
object ExtractKudu {
  val TABLE_TRAN="impala::default.LM_INSTALLMENT_TRAN"
  val TABLE_TRAN_D="impala::default.LM_INSTALLMENT_TRAN_D"
  val TABLE_MAIN="impala::default.TS_INSTALLMENT_MAIN"
  val TABLE_DETAIL="impala::default.TS_INSTALLMENT_DETAIL"
  val master="172.21.152.21,172.21.152.22,172.21.152.23"

  def tran(session:SparkSession,tableName:String,master:String)={
    val option_manager:Map[String,String]=Map(
      "kudu.table"->tableName,
      "kudu.master"->master)
    session.read.options(option_manager)
      .kudu.createOrReplaceTempView("tran")
    session.sql(
      """
        |select * from tran
      """.stripMargin).repartition(1).write.csv("D:\\Alex\\kudu\\tran")
  }
  def tran_d(session:SparkSession,tableName:String,master:String)={
    val option_manager:Map[String,String]=Map(
      "kudu.table"->tableName,
      "kudu.master"->master)
    session.read.options(option_manager)
      .kudu.createOrReplaceTempView("tran_d")
    session.sql(
      """
        |select * from tran_d
      """.stripMargin).repartition(1).write.csv("D:\\Alex\\kudu\\tran_d")
  }
  def main_s(session:SparkSession,tableName:String,master:String)={
    val option_manager:Map[String,String]=Map(
      "kudu.table"->tableName,
      "kudu.master"->master)
    session.read.options(option_manager)
      .kudu.createOrReplaceTempView("mains")
    session.sql(
      """
        |select * from mains
      """.stripMargin).repartition(1).write.csv("D:\\Alex\\kudu\\mains")
  }
  def detail(session:SparkSession,tableName:String,master:String)={
    val option_manager:Map[String,String]=Map(
      "kudu.table"->tableName,
      "kudu.master"->master)
    session.read.options(option_manager)
      .kudu.createOrReplaceTempView("detail")
    session.sql(
      """
        |select * from detail
      """.stripMargin).repartition(1).write.csv("D:\\Alex\\kudu\\detail")
  }

  def main(args: Array[String]): Unit = {
//   tran(session,TABLE_TRAN,master)
//    tran_d(session,TABLE_TRAN_D,master)
    main_s(session,TABLE_MAIN,master)
//    detail(session,TABLE_DETAIL,master)



  }






































































}
