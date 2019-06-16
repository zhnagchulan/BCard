import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}

object MysqlTest {
  val conf = new SparkConf().setAppName("mysql1").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/bear?user=root&password=1234"
  val connection = DriverManager.getConnection(url)
  Class.forName(driver)

  def main(args: Array[String]): Unit = {

    val rdd = sc.textFile("file:///C:\\Users\\user\\Desktop\\样本数据.txt", 2).map(x => {
      val regex = "(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)".r
      x match {
        case regex(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) => (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)
      }
    })
    rdd.foreachPartition(x => {
      val sql = "INSERT into zhouer VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
      val statement = connection.prepareStatement(sql)
      var num = 0
      for (i <- x) {
        statement.setString(1, i._1)
        statement.setString(2, i._2)
        statement.setString(3, i._3)
        statement.setString(4, i._4)
        statement.setString(5, i._5)
        statement.setString(6, i._6)
        statement.setString(7, i._7)
        statement.setString(8, i._8)
        statement.setString(9, i._9)
        statement.setString(10, i._10)
        statement.setString(11, i._11)
        statement.setString(12, i._12)
        statement.setString(13, i._13)
        statement.addBatch()
        num += 1
        if (num == 100) {
          statement.executeBatch()
          num = 0
        }
      }
      statement.executeBatch()
    })
  }
}

