package com.wanda.mob.spark.streaming.utils

import com.wanda.mob.spark.streaming.event.Event
import org.apache.spark.sql.Row

object Converter {

  def convertEventIntoRow(event:Event):Row ={
    Row.fromSeq(for (f<- event.getClass.getDeclaredFields) yield ReflectUtils.getAttributeValue(event, f.getName))
  }

}
