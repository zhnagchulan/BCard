package com.wanda.mob.spark.streaming.utils

/**
  * Created by siyuan.tao.wb on 2017/10/26.
  */
object StringUtils {

  /**
    * 合并字段,把同一个表相同类型的字段合并成一个字段
    *
    * @param fieldKey
    * @param fieldVal
    * @return
    */
  def mergeStr(fieldKey: Seq[String], fieldVal: Seq[String]): String = {

    if (fieldKey.length == fieldVal.length) {
      val concatStrBuf = new StringBuffer("")
      for (i <- fieldKey.indices) {
        concatStrBuf.append(fieldKey(i) + "=" + fieldVal(i) + "|")
      }
      return trimByDelimiter(concatStrBuf.toString, "|")
    }
    null
  }

  def mergeStrByStr(fieldKey: String, fieldVal: String): String = {
    fieldKey + "=" + fieldVal
  }

  def mergeStrForSql(memberCode: String, value: String): String = {
    // 注意空格
    " when " + memberCode + " then " + value
  }

  /**
    * 从拼接的字符串中提取字段
    * 备注:注意加转移符 比如 "\\|"
    *
    * @param str               字符串
    * @param CategoryDelimiter 种类分隔符
    * @param KVDelimiter       KV键值对分隔符
    * @param field             查询字段K
    * @return 字段V
    */
  def getFieldFromConcatString(str: String, CategoryDelimiter: String, KVDelimiter: String, field: String): String = {
    try {
      val fields = str.split(CategoryDelimiter)
      //            System.out.println("第一次切割的数组="+fields.toString());
      for (concatField <- fields) { //                System.out.println("提取数组的字段="+concatField);
        // searchKeywords=|clickCategoryIds=1,2,3
        if (concatField.split(KVDelimiter).length == 2) {
          val fieldName = concatField.split(KVDelimiter)(0)
          val fieldValue = concatField.split(KVDelimiter)(1)
          //                    System.out.println("KV切割K="+fieldName+"!V="+fieldValue);
          if (fieldName == field) { //                        System.out.println("最后返回"+fieldName+"验证"+field);
            return fieldValue
          }
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    null
  }

  /**
    * 判断字符串是否为空
    *
    * @param str 字符串
    * @return 是否为空
    */
  def isEmpty(str: String): Boolean = str == null || "" == str

  /**
    * 截取掉多余的字符
    *
    * @param str
    * @param delimiter
    * @return
    */
  def trimByDelimiter(str: String, delimiter: String): String = {
    if (str.startsWith(delimiter)) {
      return str.substring(1)
    }
    if (str.endsWith(delimiter)) {
      return str.substring(0, str.length() - 1)
    }
    str
  }

  /**
    * 检查字符是否为空或null
    *
    * @param str
    * @return
    */
  def isNotEmpty(str: String): Boolean = {
    str != null && "" != str
  }

}
