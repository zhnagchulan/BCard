package com.wanda.mob.spark.streaming.event.impl

import com.wanda.mob.spark.streaming.event.FinalEvent

import scala.beans.BeanProperty

case class ScoreEvent() extends FinalEvent {

  @BeanProperty var CUST_NBR: String = _
  @BeanProperty var DATE_D: String = _
  @BeanProperty var GROUPS: Int = _
  @BeanProperty var SCORE: String = _
  @BeanProperty var LST_UPD_TIME: String = _

}
