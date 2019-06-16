package com.wanda.mob.spark.streaming.event.impl

import com.wanda.mob.spark.streaming.event.BaseEvent

import scala.beans.BeanProperty

case class KafkaInputEvent() extends BaseEvent{

  @BeanProperty var CUST_NBR: String = _
  //  @BeanProperty var ACCOUNT_NMBR: String = _
  //  @BeanProperty var CASH_AMT: String = _
  //  @BeanProperty var POSTING_DTE: String = _
  @BeanProperty var LST_UPD_TIME: String = _

}