package com.wanda.mob.spark.streaming.utils

/**
  * Created by siyuan.tao.wb on 2017/11/17.
  */
class Stopwatch {

  private val start = System.currentTimeMillis()

  override def toString() = (System.currentTimeMillis() - start) / 1000.0 + "秒"
}
