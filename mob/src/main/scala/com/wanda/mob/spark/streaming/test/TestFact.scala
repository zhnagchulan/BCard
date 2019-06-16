package com.wanda.mob.spark.streaming.test

import com.wanda.mob.spark.streaming.event.FinalEvent

import scala.beans.BeanProperty

case class TestFact() extends FinalEvent {
  @BeanProperty var TRAN_CUST_NBR : String = _
  @BeanProperty var TRAN_ACCOUNT_NMBR : String = _
  @BeanProperty var TRAN_D_TERM : String = _
  @BeanProperty var TRAN_POSTING_DTE : String = _
  @BeanProperty var TRAN_STATUS : String = _
  @BeanProperty var TRAN_LST_UPD_TIME : String = _
  @BeanProperty var TRAN_POSTED_TERM : String = _
  @BeanProperty var TRAN_TOT_TERMS : String = _
  @BeanProperty var TRAN_DELQ_CYCLE : String = _
  @BeanProperty var TRAN_CRT_TIME : String = _
  @BeanProperty var TRAN_PRINCIPAL : String = _
  @BeanProperty var TRAN_D_SEQUENCE_ID :String = _
  @BeanProperty var TRAN_D_POSTING_DTE : String = _
  @BeanProperty var TRAN_D_INSTALLMENT_PLAN_ID : String = _
  @BeanProperty var TRAN_D_BILLING_DTE : String = _
  @BeanProperty var TRAN_D_BILLING_FLAG : String = _
  @BeanProperty var TRAN_D_PYMT_FLAG : String = _
  @BeanProperty var TRAN_D_LST_UPD_TIME : String = _
  @BeanProperty var TRAN_D_DELQ_STATUS : String = _
  @BeanProperty var TRAN_D_TRAN_TYPE : String = _
  @BeanProperty var TRAN_D_TRAN_AMT : String = _
  @BeanProperty var TRAN_D_TRAN_AMT_PAID : String = _
  @BeanProperty var TRAN_D_LATE_AMT : String = _
  @BeanProperty var TRAN_D_LATE_AMT_PAID : String = _
  @BeanProperty var TRAN_D_INT_AMT : String = _
  @BeanProperty var TRAN_D_INT_PAID : String = _
  @BeanProperty var TRAN_D_DELQ_CYCLE : String = _
  @BeanProperty var TRAN_D_PAYMENT_DTE : String = _
  @BeanProperty var TRAN_D_CRT_TIME : String = _
  @BeanProperty var TRAN_D_CHARGEBACK_FAIL_FEE : String = _
  @BeanProperty var TRAN_BLOCK_CODE : String = _
}
