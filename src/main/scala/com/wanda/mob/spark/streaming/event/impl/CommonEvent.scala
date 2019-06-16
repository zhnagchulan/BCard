package com.wanda.mob.spark.streaming.event.impl

import com.wanda.mob.spark.streaming.event.BaseEvent

import scala.beans.BeanProperty

case class CommonEvent() extends BaseEvent {

  @BeanProperty var CUST_NBR: String = _
  @BeanProperty var ACCOUNT_NMBR: String = _
  @BeanProperty var PYMT_FLAG: String = _
  @BeanProperty var DELQ_STATUS: String = _
  @BeanProperty var PRINCIPAL: String = _
  @BeanProperty var TRAN_AMT_PAID: String = _
  @BeanProperty var CASH_AMT: String = _
  @BeanProperty var POSTING_DTE : String = _
  @BeanProperty var STATUS: String = _
  @BeanProperty var TRANSACTION_TYPE: String = _
  @BeanProperty var PAYMENT_DTE: String = _
  @BeanProperty var LST_UPD_TIME: String = _
  @BeanProperty var AUDIT_TIME: String = _
//  CUST_NBR	ACCOUNT_NMBR	POSTING_DTE	PRINCIPAL	STATUS	TRANSACTION_TYPE	PAYMENT_DTE	LST_UPD_TIME	DELQ_STATUS	PYMT_FLAG	TRAN_AMT_PAID	CASH_AMT AUDIT_TIME
}
