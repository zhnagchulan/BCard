package com.wanda.mob.spark.streaming.utils

import com.wanda.mob.spark.streaming.event.Event
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Converter {

  def convertEventIntoRow(event:Event):Row ={
    Row.fromSeq(for (f<- event.getClass.getDeclaredFields) yield ReflectUtils.getAttributeValue(event, f.getName))
  }

  def createDataFrame(e:Event,session:SparkSession,RDDRow:RDD[Row]): DataFrame ={
    session.createDataFrame(RDDRow,ParseUtils.reflectEventAttributesToSchemaArray(e))
  }
}
