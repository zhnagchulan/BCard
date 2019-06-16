package com.wanda.mob.spark.streaming.utils

import com.wanda.mob.spark.streaming.constants.Constants
import com.wanda.mob.spark.streaming.event.Event
import org.apache.kudu.Type
import org.apache.kudu.client._
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer

object KuduUtils {

  private def getFieldResult(rowResult: RowResult, cloumnName: String): Any = {
    if (rowResult.isNull(cloumnName)) null
    else rowResult.getColumnType(cloumnName) match {
      case Type.STRING => rowResult.getString(cloumnName)
      case Type.DOUBLE => rowResult.getDouble(cloumnName)
      case Type.BOOL => rowResult.getBoolean(cloumnName)
      case Type.INT8 => rowResult.getByte(cloumnName)
      case Type.INT16 => rowResult.getShort(cloumnName)
      //      case Type.UNIXTIME_MICROS => KuduRelation.microsToTimestamp(rowResult.getLong(cloumnName))
      case Type.INT32 => rowResult.getInt(cloumnName)
      case Type.INT64 => rowResult.getLong(cloumnName)
      case Type.FLOAT => rowResult.getFloat(cloumnName)
      case Type.BINARY => rowResult.getBinaryCopy(cloumnName)
      case _ =>
        println("ERROR:getFieldResult方法解析类型错误")
    }
  }

  /**
    * 结果统一转换成String
    *
    * @param subClassOfFE
    * @param perRow
    * @tparam S
    * @return
    */
  def convertAPIRowIntoEvent[S <: Event](subClassOfFE: S, perRow: RowResult): S = {
    val event = subClassOfFE.getClass.newInstance()
    val declaredFields = event.getClass.getDeclaredFields

    for (declaredField <- declaredFields) {
      val resultAny =
        getFieldResult(perRow, declaredField.getName.toLowerCase())

      resultAny match {
        case value if value != null =>
          declaredField.setAccessible(true)
          declaredField.set(event, value.toString)

        case _ =>
      }
    }

    event
  }


  def convertSQLRowIntoEvent[T <: Event](row1: Row, row2: Row, e: T): T = {

    convertSQLRowIntoEvent(row1, convertSQLRowIntoEvent(row2, e))

  }

  /**
    *
    * sqlRow 转换成 Event
    * 注：sql最后查询出的字段一定要跟 attrName 大小写一致
    *
    * @param row
    * @param e
    * @tparam T
    * @return
    */
  def convertSQLRowIntoEvent[T <: Event](row: Row, e: T): T = {

    //防止已注入值的属性被重复注入空值
    val setedNames = ArrayBuffer[String]()

    e.getClass.getDeclaredFields.foreach(field => {

      var attr: Any = null

      row.schema.fieldNames.foreach(name => {
        if (name == field.getName.toLowerCase && !setedNames.contains(name)) {
          setedNames += name

          attr = field.getGenericType.toString match {
            case Constants.TYPE_STRING => row.getAs[String](name)
            //        case Constants.TYPE_INT => println("int@@@@@@@@@@@@@")
            //          row.getAs[Int](attrName)
            //        case Constants.TYPE_DOUBLE => println("double@@@@@@@@@@@@@@@@")
            //          row.getAs[Double](attrName)
            //        case Constants.TYPE_LONG => println("long!@@@@@@@@@@@@@@@@@@@@@")
            //          row.getAs[Long](attrName)
            case _ =>
          }
        }
      })

      if (attr != null) {
        field.setAccessible(true)
        field.set(e, attr.toString)
      }
    })

    e
  }


}
