package com.wanda.mob.spark.streaming.accumulator.factory

import com.wanda.mob.spark.streaming.accumulator.CustomHashSetAccumulator
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/**
  * Created by siyuan.tao.wb on 2017/12/1.
  */
class AccumulatorFactory[T:ClassTag] {

  @volatile private var instance: CustomHashSetAccumulator[T] = _

  def getHashSetAccInstance(sc: SparkContext): CustomHashSetAccumulator[T] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val acc = new CustomHashSetAccumulator[T]()
          sc.register(acc,"HashSetAccumulator")
          instance = acc
        }
      }
    }
    instance
  }
}
