package com.wanda.mob.spark.streaming.utils

import com.wanda.mob.spark.streaming.constants.Constants
import com.wanda.mob.spark.streaming.event.Event
import org.apache.spark.sql.types._

object ReflectUtils {

  /**
    * 反射出对应Key的值
    * 备注:Scala的反射跟Java的反射有区别,需要反射的类属性需要 @BeanProperty
    *
    * @param thisClass 需要获取属性值的类
    * @param fieldName 该类的属性名
    * @return
    */
  def getAttributeValue[T <: Event](thisClass: T, fieldName: String): AnyRef = {
    try {

      val upperCaseField = fieldName.toUpperCase()
      val method = thisClass.getClass.getMethod("get" + upperCaseField)
      method.invoke(thisClass)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        print(s"反射抽取属性值错误---${thisClass.getClass.getSimpleName}")
        null
    }
  }

  def setAttrByConcatStr[T <: Event](thisClass: T,
                                     concatName: String,
                                     concatValue: String): T = {

    val fields = thisClass.getClass.getDeclaredFields
    val names = concatName.split(";")
    val values = concatValue.split(";")

    // 这里赋值要保证names和values的字段一一对应
    fields.foreach(field => {
      for (index <- 0 until names.length if field.getName == names(index)) {
          field.setAccessible(true)
          field.set(thisClass, values(index))
      }
    })
    thisClass
  }

  /**
    * 抽取thisClass的相同字段的属性到对应的otherClass中
    * 条件是:相同字段大写，格式和类型一致
    * @param thisClass
    * @param otherClass
    * @tparam T
    * @tparam O
    * @return
    */
  def setAttrByEvent[T<:Event,O<:Event](thisClass: T,otherClass:O): O ={

    val thisFields = thisClass.getClass.getDeclaredFields
    val otherFields = otherClass.getClass.getDeclaredFields

    thisFields.foreach(thisField=>{
      for(otherField <- otherFields if thisField.getName.toUpperCase() == otherField.getName.toUpperCase()){
        val value = getAttributeValue(thisClass,thisField.getName)
        if (value !=null){
          otherField.setAccessible(true)
          otherField.set(otherClass, value)
        }
      }
    })
    otherClass
  }

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
        case Constants.TYPE_STRING => StructField(field.getName.toLowerCase, StringType, nullable = true)
        case Constants.TYPE_DOUBLE => StructField(field.getName.toLowerCase, DoubleType, nullable = true)
        case Constants.TYPE_INT => StructField(field.getName.toLowerCase, IntegerType, nullable = true)
        case Constants.TYPE_LONG => StructField(field.getName.toLowerCase, LongType, nullable = true)
      }
    })
  }

}
