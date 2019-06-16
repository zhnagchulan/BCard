import com.wanda.mob.spark.streaming.kudu.factory.DAOFactory
import com.wanda.mob.spark.streaming.utils.KuduUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Test {
  private val _KUDU_MASTER = "192.168.9.32,192.168.9.31,192.168.9.30"
  private val Test: String = "impala::default.siyuan_test1"
  private val Test2: String = "impala::default.siyuan_test2"

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("B Card Model Daily Calculation")
    conf.set("spark.local.dir","D:\\shuffle_dir")

    //    val sc = new SparkContext(conf)
    val sc = new SparkContext("local[*]", "test111", conf)

    sc.setLogLevel("WARN")
    //    sc.setCheckpointDir("D:\\check-point")
    val session = SparkSession.builder().getOrCreate()

    val test: Map[String, String] = Map(
      "kudu.table" -> Test,
      "kudu.master" -> _KUDU_MASTER)
    val test2: Map[String, String] = Map(
      "kudu.table" -> Test2,
      "kudu.master" -> _KUDU_MASTER)

    val aa = DAOFactory.getKuduInteractionLayer.doExtractBySQL(

          t => {
            s"""
               |SELECT *
               |from $t
            """.stripMargin
          }, test, session
        )
      .rdd.map(a=>{
          (a.getString(0),a)
      })

    val bb = DAOFactory.getKuduInteractionLayer.doExtractBySQL(

      t => {
        s"""
           |SELECT *
           |from $t
            """.stripMargin
      }, test2, session
    )
      .rdd.map(a=>{
      (a.getString(0),a)
    })

    aa.join(bb)
      .map(tt=>{
        val t1 = tt._2._1
        val t2 = tt._2._2

        KuduUtils.convertSQLRowIntoEvent(t1,t2,Tee())

      })
      .foreach(a=>{
        println("result="+a.AID,a.BBB,a.CCC,a.NO,a.OK)
      })

  }




}
