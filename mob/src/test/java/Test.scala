import java.text.SimpleDateFormat

import com.wanda.mob.spark.streaming.utils.Stopwatch
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Test {
  private val _KUDU_MASTER = "172.21.152.21,172.21.152.22,172.21.152.23"

  /**
    * B卡模型
    */

  private val _B_CREDIT_rt_GRADE: String = "impala::default.b_cm_rt_grade"

  /**
    * 交易主表
    */

  private val _LM_INSTALLMENT_TRAN: String = "impala::default.LM_INSTALLMENT_TRAN"
  private val _TS_INSTALLMENT_MAIN: String = "impala::default.TS_INSTALLMENT_MAIN"

  private val _B_CMS_BOX: String = "impala::default.b_cms_box"

  /**
    * 交易明细
    */

  private val _LM_INSTALLMENT_TRAN_D: String = "impala::default.LM_INSTALLMENT_TRAN_D"
  private val _TS_INSTALLMENT_DETAIL: String = "impala::default.TS_INSTALLMENT_DETAIL"

  /**
    * 注册表
    */

  private val _FCS_ACCT_REGISTER: String = "impala::default.FCS_ACCT_REGISTER"

  /**
    * 鹏元学历
    */

  private val _T_CTM_KYH_PBOC_REQ: String = "impala::default.t_ctm_kyh_pboc_req"

  /**
    * ttttttttttttttttttt
    */

  private val tttttt: String = "impala::default.test11"

  /**
    * 过滤所需表
    */

  private val Delete: String = "b_cm_ol_grade"

  def main(args: Array[String]): Unit = {


    val totalProcessingTime = new Stopwatch()

    val conf = new SparkConf().setAppName("B Card Model Daily Calculation")
    conf.set("spark.local.dir","D:\\shuffle_dir")

    //    val sc = new SparkContext(conf)
    val sc = new SparkContext("local[*]", "test111", conf)

    sc.setLogLevel("WARN")
    //    sc.setCheckpointDir("D:\\check-point")
    val session = SparkSession.builder().getOrCreate()
    //    val kc = new KuduContext(kuduMaster, sc)

    val kc = new KuduContext(_KUDU_MASTER, sc)

    //    val b_ol_grade: Map[String, String] = Map(
    //      "kudu.table" -> _B_CREDIT_OL_GRADE,
    //      "kudu.master" -> _KUDU_MASTER)

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

    val degree: Map[String, String] = Map(
      "kudu.table" -> _T_CTM_KYH_PBOC_REQ,
      "kudu.master" -> _KUDU_MASTER)

    val b_box: Map[String, String] = Map(
      "kudu.table" -> _B_CMS_BOX,
      "kudu.master" -> _KUDU_MASTER)
    //    kc.deleteTable("impala::default.b_cm_rt_offset")

    //    val a = "a,b,c,d,e"
    //    DAOFactory.getKuduInteractionLayer.deleteRows(
    //      table => {
    //        s"""
    //           |select cust_nbr,date_m from $table
    //           |where cust_nbr in ('a','b','c','d','e')
    //           |and date_m = 'a_m'
    //                """.stripMargin
    //      },
    //      session,
    //      b_box,
    //      _B_CMS_BOX,
    //      kc)

    //    DAOFactory.getKuduInteractionLayer.doExtractBySQL(
    //      b_b => {
    //        s"""
    //           |SELECT *
    //          |from $b_b where cust_nbr in ($a)
    //        """.stripMargin
    //      }, b_box, session
    //    ).foreach(row=>println(row.toString()))

    kc.deleteTable(tttttt)

    println("耗时:"+totalProcessingTime)
  }

  import org.apache.kudu.spark.kudu._

  def deleteRows(sqlFuc: String => String,
                 session: SparkSession,
                 kuduOptions: Map[String, String],
                 tableName: String,
                 kc: KuduContext): Unit = {

    val tmpTable = "tmp_table"

    session
      .read
      .options(kuduOptions)
      .kudu
      .createOrReplaceTempView(tmpTable)

    val sqlStr = sqlFuc(tmpTable)
    // 必须以主键删除
    val deleteDF = session.sql(sqlStr)

    kc.deleteRows(deleteDF, tableName)
  }

  def createTable(kc:KuduContext,tableName:String): Unit ={
    import org.apache.kudu.Type
    import org.apache.kudu.ColumnSchema
    import org.apache.kudu.client.CreateTableOptions
    import java.util
    // Set up a simple schema.// Set up a simple schema.

    val columns = new util.ArrayList[ColumnSchema]()
    columns.add(new ColumnSchema.ColumnSchemaBuilder("CUST_NBR", Type.STRING).key(true).build)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("MAX_O_DAYS", Type.STRING).build)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("O_TIMES", Type.STRING).build)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("RATE_OF_INCREASE", Type.STRING).build)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("RECENTLY_TRAN_DAYS", Type.STRING).build)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("DEGREE", Type.STRING).build)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("CONTACTS", Type.STRING).build)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("MAX_O_DAYS_BOX", Type.INT32).build)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("O_TIMES_BOX", Type.INT32).build)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("RATE_OF_INCREASE_BOX", Type.INT32).build)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("RECENTLY_TRAN_DAYS_BOX", Type.INT32).build)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("DEGREE_BOX", Type.INT32).build)
    columns.add(new ColumnSchema.ColumnSchemaBuilder("CONTACTS_BOX", Type.INT32).build)
    val schema = new Schema(columns)
    // Set up the partition schema, which distributes rows to different tablets by hash.
    // Kudu also supports partitioning by key range. Hash and range partitioning can be combined.
    // For more information, see http://kudu.apache.org/docs/schema_design.html.
    val cto = new CreateTableOptions
    val hashKeys = new util.ArrayList[String](1)
    hashKeys.add("CUST_NBR")
    val numBuckets = 5
    cto.addHashPartitions(hashKeys, numBuckets)

    val client = kc.syncClient
    // Create the table.
    client.createTable(tableName, schema, cto)
  }
}
