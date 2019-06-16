package com.wanda.mob.spark.streaming.utils

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.wanda.mob.spark.streaming.constants.Constants
import com.wanda.mob.spark.streaming.event.Event
import com.wanda.mob.spark.streaming.event.impl.KafkaInputEvent
import org.apache.spark.sql.types._


/**
  * Created by siyuan.tao.wb on 2017/10/24.
  */
object ParseUtils {


  /**
    * 反射出event属性并解析成DF需要的StructField数组
    *
    * @param event
    * @return
    */
  def reflectEventAttributesToSchemaArray(event: Event): StructType = {
    val fields = event.getClass.getDeclaredFields

    StructType(for {
      field <- fields
    } yield {
      // Todo 增加额外匹配 防止报错
      field.getGenericType.toString match {
        case Constants.TYPE_DOUBLE => StructField(field.getName.toLowerCase, DoubleType, nullable = true)
        case Constants.TYPE_INT => StructField(field.getName.toLowerCase, IntegerType, nullable = true)
        case Constants.TYPE_LONG => StructField(field.getName.toLowerCase, LongType, nullable = true)
        case Constants.TYPE_STRING => StructField(field.getName.toLowerCase, StringType, nullable = true)
      }
    })
  }

  /**
    * 反射出对应Key的值
    * 备注:Scala的反射跟Java的反射有区别,需要反射的类属性需要 @BeanProperty
    *
    * @param thisClass 需要获取属性值的类
    * @param fieldName 该类的属性名称
    * @return
    */
  def getAttributeValue[T <: Event](thisClass: T, fieldName: String): AnyRef = {
    try {
      // 防止首字母无大写 后面get不到
      val methodName = fieldName.substring(0, 1).toUpperCase + fieldName.substring(1)
      val method = thisClass.getClass.getMethod("get" + methodName)
      method.invoke(thisClass)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        print(Constants.ERROR_REFLECT_VALUE_FLAG + fieldName)
        Constants.ERROR_REFLECT_VALUE_FLAG
    }
  }

  def getOffset(set: java.util.Set[String]): String = {
    import scala.collection.JavaConversions._
    var offset: String = null

    if (set.size() == 1) {
      for (element <- set) {
        offset = element
      }
    } else {
      println("ERROR: offset 不唯一")
      System.exit(0)
    }
    offset
  }


  def getCustNumsFromSet(set: java.util.Set[String]): String = {
    import scala.collection.JavaConversions._

    var concatStr = ""

    for (element <- set) {
      concatStr = concatStr.concat(element + ",")
    }
    StringUtils.trimByDelimiter(concatStr, ",")

  }

  /**
    * 解析从Kafka拉取过来的Json格式的数据 并封装成对应Bean返回
    * Gson方法中的类型必须写死
    *
    * @param jsonStr
    * @return
    */
  def parseJsonToRTInputEvent(jsonStr: String): Option[KafkaInputEvent] = {

    try {
      val gson = new Gson()
      val mapType = new TypeToken[KafkaInputEvent] {}.getType
      Some(gson.fromJson[KafkaInputEvent](jsonStr, mapType))
    } catch {
      case _: Exception =>
        println("Parse Json Error " + jsonStr)
        None
    }
  }
}