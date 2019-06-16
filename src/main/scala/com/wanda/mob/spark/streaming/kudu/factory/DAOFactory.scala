package com.wanda.mob.spark.streaming.kudu.factory

import com.wanda.mob.spark.streaming.kudu.KuduInteractionLayer


object DAOFactory {

  def getKuduInteractionLayer: KuduInteractionLayer ={
    new KuduInteractionLayer()
  }
}
