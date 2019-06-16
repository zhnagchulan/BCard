package com.wanda.mob.spark.streaming.accumulator

import java.util
import java.util.Collections

import org.apache.spark.util.AccumulatorV2

/**
  * Created by siyuan.tao.wb on 2017/12/1.
  */
class CustomHashSetAccumulator[T] extends AccumulatorV2[T, java.util.Set[T]]{

  private val _set: java.util.Set[T] = Collections.synchronizedSet(new util.HashSet[T]())

  override def isZero: Boolean = _set.isEmpty

  override def copyAndReset(): CustomHashSetAccumulator[T] = new CustomHashSetAccumulator

  override def copy(): CustomHashSetAccumulator[T] = {
    val newAcc = new CustomHashSetAccumulator[T]
    _set.synchronized {
      newAcc._set.addAll(_set)
    }
    newAcc
  }

  override def reset(): Unit = _set.clear()

  override def add(v: T): Unit = _set.add(v)

  override def merge(other: AccumulatorV2[T, java.util.Set[T]]): Unit = other match {
    case o: CustomHashSetAccumulator[T] => _set.addAll(o.value)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  // 不能在这里面直接提取具体值,出去提取
  override def value: java.util.Set[T] = _set.synchronized {
    java.util.Collections.unmodifiableSet(new util.HashSet[T](_set))
  }

//  private[spark] def setValue(newValue: java.util.Set[T]): Unit = {
//    _set.clear()
//    _set.addAll(newValue)
//  }

  def getTotalNbr: Int = _set.synchronized {
    _set.size()
  }
}
