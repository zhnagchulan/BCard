import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Date

import com.wanda.mob.spark.streaming.event.Event
import com.wanda.mob.spark.streaming.event.impl.ScoreEvent
import com.wanda.mob.spark.streaming.main.Offline._
import com.wanda.mob.spark.streaming.mysql.MConnector
import com.wanda.mob.spark.streaming.utils.Converter
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object TestTime {
  private val upsertMysqlURL= "jdbc:mysql://172.21.230.18:3306/crd?user=crduser&password=MZTWhdJdH0Ypvnx1"
  private val mysql_url = "jdbc:mysql://172.21.230.18:3306/crd?useUnicode=true&characterEncoding=utf8"
  private val driver = "com.mysql.jdbc.Driver"
  private val username = "crduser"
  private val password = "MZTWhdJdH0Ypvnx1"

  private val mysql_table = "b_cm_box"
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("B Creadit Model Daily Calculation")

        val sc = new SparkContext("local[4]", "test111", conf)
        conf.set("spark.local.dir", "D:\\shuffle_dir")
        sc.setLogLevel("WARN")
//    val sc = new SparkContext(conf)

    val session: SparkSession = SparkSession.builder().getOrCreate()

    val ff = sc.parallelize(Seq(1,2))
      .map(a=>{
        val event = ScoreEvent()
        event.LST_UPD_TIME="55555===="
        event.SCORE = "55555"
        event.DATE_D ="2018-01-05"
        event.MEMBER_CODE = a.toString
        event.GROUPS = 5
        event
//        Converter.convertEventIntoRow(event)
      })

    upsertScoreEvent(ff,upsertMysqlURL,mysql_table)

//    val df = Converter.createDataFrame(ScoreEvent(), session, ff)
//    new MConnector().writeToMysqlByAppend(df, mysql_table, mysql_url, driver, username, password)
  }

  val connection: Connection = DriverManager.getConnection(upsertMysqlURL)

  def upsertScoreEvent(rdd:RDD[ScoreEvent],mysqlURL:String,tableName:String): Unit ={
    rdd.foreachPartition(iter=>{

      val sql = s"insert into $tableName values(?,?,?,?,?) ON DUPLICATE KEY UPDATE GROUPS=?,SCORE=?,LST_UPD_TIME=?"


      var parNum = 0
      val statement = connection.prepareStatement(sql)

      iter.foreach(e=>{

        statement.setString(1,e.MEMBER_CODE)
        statement.setString(2,e.DATE_D)
        statement.setInt(3,e.GROUPS)
        statement.setString(4,e.SCORE)
        statement.setString(5,e.LST_UPD_TIME)
        statement.setInt(6,e.GROUPS)
        statement.setString(7,e.SCORE)
        statement.setString(8,e.LST_UPD_TIME)
        statement.addBatch()
        parNum +=1
        if (parNum >=100){
          statement.executeBatch()
          parNum = 0
        }
      })
      statement.executeBatch()
    })
  }
}
