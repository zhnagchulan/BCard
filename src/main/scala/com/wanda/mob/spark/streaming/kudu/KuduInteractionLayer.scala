package com.wanda.mob.spark.streaming.kudu

import com.wanda.mob.spark.streaming.event.Event
import com.wanda.mob.spark.streaming.utils.{KuduUtils, ReflectUtils, Stopwatch}
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

class KuduInteractionLayer {


  private def registerSession(session: SparkSession,
                              kuduOptions: Map[String, String],
                              tmpTable: String): Unit = {

    session
      .read
      .options(kuduOptions)
      .kudu
      .createOrReplaceTempView(tmpTable)
  }
  def doExtractBySQL(sqlFuc: (String, String, String) => String,
                     kuduOptionsA: Map[String, String],
                     kuduOptionsB: Map[String, String],
                     kuduOptionsC: Map[String, String],
                     session: SparkSession): DataFrame = {

    val table_a = "tmp_a"
    val table_b = "tmp_b"
    val table_c = "tmp_c"

    registerSession(session, kuduOptionsA, table_a)
    registerSession(session, kuduOptionsB, table_b)
    registerSession(session, kuduOptionsC, table_c)

    val sqlStr = sqlFuc(table_a, table_b, table_c)

    session.sql(sqlStr)
  }

  def doExtractBySQL(sqlFuc: (String, String, String, String) => String,
                     kuduOptionsA: Map[String, String],
                     kuduOptionsB: Map[String, String],
                     kuduOptionsC: Map[String, String],
                     kuduOptionsD: Map[String, String],
                     session: SparkSession): DataFrame = {

    val table_a = "tmp_a"
    val table_b = "tmp_b"
    val table_c = "tmp_c"
    val table_d = "tmp_d"

    registerSession(session, kuduOptionsA, table_a)
    registerSession(session, kuduOptionsB, table_b)
    registerSession(session, kuduOptionsC, table_c)
    registerSession(session, kuduOptionsD, table_d)

    val sqlStr = sqlFuc(table_a, table_b, table_c, table_d)

    session.sql(sqlStr)
  }

  def doExtractBySQL(sqlFuc: (String, String, String, String, String, String) => String,
                     kuduOptionsA: Map[String, String],
                     kuduOptionsB: Map[String, String],
                     kuduOptionsC: Map[String, String],
                     kuduOptionsD: Map[String, String],
                     kuduOptionsE: Map[String, String],
                     kuduOptionsF: Map[String, String],
                     session: SparkSession): DataFrame = {

    val table_a = "tmp_a"
    val table_b = "tmp_b"
    val table_c = "tmp_c"
    val table_d = "tmp_d"
    val table_e = "tmp_e"
    val table_f = "tmp_f"

    registerSession(session, kuduOptionsA, table_a)
    registerSession(session, kuduOptionsB, table_b)
    registerSession(session, kuduOptionsC, table_c)
    registerSession(session, kuduOptionsD, table_d)
    registerSession(session, kuduOptionsE, table_e)
    registerSession(session, kuduOptionsF, table_f)

    val sqlStr = sqlFuc(table_a, table_b, table_c, table_d, table_e, table_f)

    session.sql(sqlStr)
  }

  def doExtractBySQL(sqlFuc: (String, String) => String,
                     kuduOptionsA: Map[String, String],
                     kuduOptionsB: Map[String, String],
                     session: SparkSession): DataFrame = {

    val table_a = "tmp_a"
    val table_b = "tmp_b"

    registerSession(session, kuduOptionsA, table_a)
    registerSession(session, kuduOptionsB, table_b)

    val sqlStr = sqlFuc(table_a, table_b)

    session.sql(sqlStr)
  }

  def doExtractBySQL(sqlFuc: String => String,
                     kuduOptions: Map[String, String],
                     session: SparkSession): DataFrame = {

    //    val stopwatch = new Stopwatch()
    //    print(" 开始抽取" + kuduOptions("kudu.table"))

    val tmpTable = "tmp_table"

    session
      .read
      .options(kuduOptions)
      .kudu
      .createOrReplaceTempView(tmpTable)

    val sqlStr = sqlFuc(tmpTable)
    val resultDF = session.sql(sqlStr)

    //    println(s"抽取完毕，耗时$stopwatch")
    resultDF
  }

  def doUpsertByDefaultAPI(rddRow: RDD[Row],
                           e: Event,
                           tableName: String,
                           session: SparkSession,
                           kuduContext: KuduContext): Unit = {
    val structType = ReflectUtils.reflectEventAttributesToSchemaArray(e)

    val df = session.createDataFrame(rddRow, structType)

    df match {
      case nonEmptyDF if !nonEmptyDF.rdd.isEmpty() =>
        print("开始更新插入" + tableName)
        val stopwatch = new Stopwatch()

        kuduContext.upsertRows(nonEmptyDF, tableName)

        println(s"插入更新Kudu完毕,耗时$stopwatch")
      case _ =>
        println(s"WRNING:插入到$tableName 表的DF为空,其RDD为${df.rdd.name},ID为${df.rdd.id}")
    }
  }

  //  def doExtractByListPredicateAPI[
  //  S <: Event](givenColumns:ArrayBuffer[String],
  //              predicateColumn: String,
  //              predicateValues: Iterator[String],
  //              subClassOfFE: S,
  //              tableName: String,
  //              kuduContext: KuduContext): ArrayBuffer[S] = {
  //    import scala.collection.JavaConverters._
  //
  //    val client = kuduContext.syncClient
  //    val table = client.openTable(tableName)
  //    val schema = table.getSchema
  //
  //    val initialzedKuduScanner = client.newScannerBuilder(table)
  //
  //    val listPre = KuduPredicate.newInListPredicate(schema.getColumn(predicateColumn),predicateValues.toList.asJava)
  //
  //    val periodTimePredicate = KuduPredicate.newComparisonPredicate(schema.getColumn("approve_period"),
  //      KuduPredicate.ComparisonOp.EQUAL,
  //      periodTime)
  //
  //    val scanner = initialzedKuduScanner
  //      .setProjectedColumnNames(givenColumns.asJava)
  //      .addPredicate(listPre)
  //      .batchSizeBytes(1024 * 1024 * 20)
  //      .build()
  //
  //    val eventArray = new ArrayBuffer[S]()
  //
  //    try {
  //
  //      while (scanner.hasMoreRows) {
  //        val results = scanner.nextRows()
  //        while (results.hasNext) {
  //          eventArray.append(KuduUtils.convertAPIRowIntoEvent[S](subClassOfFE, results.next()))
  //        }
  //      }
  //    } catch {
  //      case e: KuduException =>
  //        print("doExtract方法抽取Kudu错误")
  //        e.printStackTrace()
  //    } finally {
  //      scanner.close()
  //    }
  //
  //    eventArray
  //  }

  private def doExtract[S <: Event](subClassOfFE: S,
                                    scanner: KuduScanner): ArrayBuffer[S] = {

    val eventArray = new ArrayBuffer[S]()

    try {

      while (scanner.hasMoreRows) {
        val results = scanner.nextRows()
        while (results.hasNext) {
          eventArray.append(KuduUtils.convertAPIRowIntoEvent[S](subClassOfFE, results.next()))
        }
      }
    } catch {
      case e: KuduException =>
        print("doExtract方法抽取Kudu错误")
        e.printStackTrace()
    } finally {
      scanner.close()
    }

    eventArray
  }


  def doExtractForTD[
  S <: Event](givenColumns: ArrayBuffer[String],
              predicateColumn: String,
              predicateValues: ArrayBuffer[String],
              subClassOfFE: S,
              tableName: String,
              kuduContext: KuduContext): ArrayBuffer[S] = {
    import scala.collection.JavaConverters._

    val client = kuduContext.syncClient
    val table = client.openTable(tableName)
    val schema = table.getSchema

    val initialzedKuduScanner = client.newScannerBuilder(table)

    val accountListPre = KuduPredicate.newInListPredicate(schema.getColumn(predicateColumn), predicateValues.asJava)

    val tranTypePredicate = KuduPredicate.newComparisonPredicate(schema.getColumn("tran_type"),
      KuduPredicate.ComparisonOp.EQUAL,
      "4000")

    val buf = ArrayBuffer[String]()
    buf.append("00")
    buf.append("01")
    val pymtFlagList = KuduPredicate.newInListPredicate(schema.getColumn("pymt_flag"), buf.asJava)

    val scanner = initialzedKuduScanner
      .setProjectedColumnNames(givenColumns.asJava)
      .addPredicate(accountListPre)
      .addPredicate(tranTypePredicate)
      .addPredicate(pymtFlagList)
      .batchSizeBytes(1024 * 1024 * 20)
      .build()

    doExtract(subClassOfFE, scanner)
  }

  def doExtractForTDetail[
  S <: Event](givenColumns: ArrayBuffer[String],
              predicateColumn: String,
              predicateValues: ArrayBuffer[Long],
              subClassOfFE: S,
              tableName: String,
              kuduContext: KuduContext): ArrayBuffer[S] = {
    import scala.collection.JavaConverters._

    val client = kuduContext.syncClient
    val table = client.openTable(tableName)
    val schema = table.getSchema

    val initialzedKuduScanner = client.newScannerBuilder(table)

    val accountListPre = KuduPredicate.newInListPredicate(schema.getColumn(predicateColumn), predicateValues.asJava)


    val buf = ArrayBuffer[Int]()
    buf.append(0)
    buf.append(1)
    buf.append(2)
    buf.append(4)

    val statusList = KuduPredicate.newInListPredicate(schema.getColumn("status"), buf.asJava)

    val scanner = initialzedKuduScanner
      .setProjectedColumnNames(givenColumns.asJava)
      .addPredicate(accountListPre)
      .addPredicate(statusList)
      .batchSizeBytes(1024 * 1024 * 20)
      .build()

    doExtract(subClassOfFE, scanner)
  }


  def doExtractForRegsiter[
  S <: Event](givenColumns: ArrayBuffer[String],
              predicateColumn: String,
              predicateValues: Iterator[String],
              subClassOfFE: S,
              tableName: String,
              kuduContext: KuduContext): ArrayBuffer[S] = {
    import scala.collection.JavaConverters._

    val client = kuduContext.syncClient
    val table = client.openTable(tableName)
    val schema = table.getSchema

    val initialzedKuduScanner = client.newScannerBuilder(table)

    val custNumListPre = KuduPredicate.newInListPredicate(schema.getColumn(predicateColumn), predicateValues.toList.asJava)

    val scanner = initialzedKuduScanner
      .setProjectedColumnNames(givenColumns.asJava)
      .addPredicate(custNumListPre)
      .batchSizeBytes(1024 * 1024 * 20)
      .build()

    doExtract(subClassOfFE, scanner)
  }

  def doExtractForTran[
  S <: Event](givenColumns: ArrayBuffer[String],
              predicateColumn: String,
              predicateValues: ArrayBuffer[String],
              subClassOfFE: S,
              tableName: String,
              kuduContext: KuduContext): ArrayBuffer[S] = {
    import scala.collection.JavaConverters._

    val client = kuduContext.syncClient
    val table = client.openTable(tableName)
    val schema = table.getSchema

    val initialzedKuduScanner = client.newScannerBuilder(table)

    val custNumListPre = KuduPredicate.newInListPredicate(schema.getColumn(predicateColumn), predicateValues.asJava)

    val tranTypePredicate = KuduPredicate.newComparisonPredicate(schema.getColumn("transaction_type"),
      KuduPredicate.ComparisonOp.EQUAL,
      "3")

    val scanner = initialzedKuduScanner
      .setProjectedColumnNames(givenColumns.asJava)
      .addPredicate(custNumListPre)
      .addPredicate(tranTypePredicate)
      .batchSizeBytes(1024 * 1024 * 20)
      .build()

    doExtract(subClassOfFE, scanner)
  }

  def doExtractForMain[
  S <: Event](givenColumns: ArrayBuffer[String],
              predicateColumn: String,
              predicateValues: ArrayBuffer[String],
              subClassOfFE: S,
              tableName: String,
              kuduContext: KuduContext): ArrayBuffer[S] = {
    import scala.collection.JavaConverters._

    val client = kuduContext.syncClient
    val table = client.openTable(tableName)
    val schema = table.getSchema

    val initialzedKuduScanner = client.newScannerBuilder(table)

    val custNumListPre = KuduPredicate.newInListPredicate(schema.getColumn(predicateColumn), predicateValues.asJava)

    val tranTypePredicate = KuduPredicate.newComparisonPredicate(schema.getColumn("txn_type"),
      KuduPredicate.ComparisonOp.EQUAL,
      1)

    val buf = ArrayBuffer[Int]()
    buf.append(0)
    buf.append(1)
    buf.append(4)

    val statusFlagList = KuduPredicate.newInListPredicate(schema.getColumn("status"), buf.asJava)

    val scanner = initialzedKuduScanner
      .setProjectedColumnNames(givenColumns.asJava)
      .addPredicate(custNumListPre)
      .addPredicate(statusFlagList)
      .addPredicate(tranTypePredicate)
      .batchSizeBytes(1024 * 1024 * 20)
      .build()

    doExtract(subClassOfFE, scanner)
  }

  //    def doUpsertBasicOnEventIter[T <: FinalEvent](eventItr: Iterator[T],
  //                                                 tableName: String,
  //                                                 kuduContext: KuduContext,
  //                                                 operationType: String): Unit = {
  //
  //      val client = kuduContext.syncClient
  //      val session = client.newSession
  //      //AUTO_FLUSH_BACKGROUND效率不及MANUAL_FLUSH，所以用setMutationBufferSpace
  //      //Todo 生产根据情况调整
  //      session.setFlushMode(FlushMode.MANUAL_FLUSH)
  //      //条数以一定要比foreach里面的条数多，不然会报MANUAL_FLUSH is enabled but the buffer is too big
  //      session.setMutationBufferSpace(Constants.KUDU_MUTATION_BUFFER_SPACE)
  //
  //      val table = client.openTable(tableName)
  //
  //
  //      if (operationType == Constants.KUDU_OPERATION_UPSERT) {
  //
  //        try {
  //          for (e <- eventItr) {
  //            val upsert = table.newUpsert()
  //            val row = upsert.getRow
  //            KuduUtils.setRow(row, e)
  //            session.apply(upsert)
  //          }
  //          session.flush()
  //        } catch {
  //          case e: KuduException =>
  //            e.printStackTrace()
  //        } finally try
  //          session.close
  //        catch {
  //          case e: KuduException =>
  //            e.printStackTrace()
  //        }
  //      } else if (operationType == Constants.KUDU_OPERATION_UPDATE) {
  //        try {
  //          for (e <- eventItr) {
  //            val update = table.newUpdate()
  //            val row = update.getRow
  //            KuduUtils.updateRow(row, e)
  //            session.apply(update)
  //          }
  //          session.flush()
  //        } catch {
  //          case e: KuduException =>
  //            e.printStackTrace()
  //        } finally try
  //          session.close
  //        catch {
  //          case e: KuduException =>
  //            e.printStackTrace()
  //        }
  //      } else {
  //        println("错误：请输入正确的kudu操作类型")
  //      }
  //
//  if (session.countPendingErrors()!=0) {
//    val pendingErrors = session.getPendingErrors
//    val errorCount = pendingErrors.getRowErrors.length
//    if (errorCount > 0) {
//      val errors = pendingErrors.getRowErrors.take(5).map(_.getErrorStatus).mkString
//      throw new RuntimeException(
//        s"警告，写入 $errorCount rows from DataFrame to Kudu失败; sample errors: $errors"
//      )
//    }
//
//    if (pendingErrors.isOverflowed) System.out.println("error buffer overflowed: some errors were discarded")
//    throw new RuntimeException("error inserting rows to Kudu")
//  }
  //    }

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
}
