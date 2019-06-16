package com.wanda.mob.spark.streaming.constants

object Constants {


  /**
    * Kudu配置属性
    */

  val KUDU_MASTER: String = "172.21.152.21,172.21.152.22,172.21.152.23"


  /**
    * 错误标识
    */

  val PARSE_JSON_ERROR_FLAG: String = "Parse Json Error"

  val FORMART_IP_ERROR: String = "Format IP Error,Original IP is:"

  val ERROR_COMPARE_FLAG: String = "getNewestOneError"

  val ERROR_REFLECT_VALUE_FLAG: String = "Reflect Value Error = "

  val ERROR_UPDATED_FDEVENT_IS_EMPTY: Boolean = true

  val ERROR_AGGR_FLAG: String = "empty"

  val ERROR_TIME_FORMAT:String = "异常时间"


  /**
    * 类型标识
    */

  val TYPE_INT: String = "int"

  val TYPE_DOUBLE: String = "double"

  val TYPE_LONG: String = "long"

  val TYPE_STRING: String = "class java.lang.String"



}
