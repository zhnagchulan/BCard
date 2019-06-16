package Test

import java.text.SimpleDateFormat

object t {

  def main(args: Array[String]): Unit = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    println(    dateFormat.format(1542641581000L)
    )
  }
}
