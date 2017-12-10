package com.ptb.gaia.common.service

import org.apache.spark.sql.DataFrame

/**
  * Created by MengChen on 2017/2/8.
  */
trait IProcess {

  def saveWxDataToMongoLr(saveDf: DataFrame)

}
