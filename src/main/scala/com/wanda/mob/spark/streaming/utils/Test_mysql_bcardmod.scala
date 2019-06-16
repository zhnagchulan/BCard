package com.wanda.mob.spark.streaming.utils
import java.util.Properties

import com.wanda.mob.spark.streaming.main.New_Offline.session
object Test_mysql_bcardmod {
  private val driver = "com.mysql.jdbc.Driver"
  private val username = "bcardmod"
  private val password = "GGCv5%#@XSE6Yh3K"

//  private val upsertMysqlURL= "jdbc:mysql://172.21.230.18:3306/bcardmod?useUnicode=true&characterEncoding=utf8"
private val upsertMysqlURL= "jdbc:mysql://172.21.230.18:3306/bcardmod?user=bcardmod&password=GGCv5%25%23@XSE6Yh3K"
  Class.forName(driver)
  val properties=new Properties()
//  properties.setProperty("user",username)
//  properties.setProperty("password",password)
  def main(args: Array[String]): Unit = {
    session.read.jdbc(upsertMysqlURL,"b_mm_box",properties)
        .show()

  }


}
