package com.ptb.gaia.tools

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MengChen on 2016/5/13.
  */
class Tools {

  def runJob(job: (Array[String], SparkContext) => Unit, args: Array[String],
    context: SparkContext, errorMsg: String = "任务执行失败"): Unit = {
    try {
      job(args, context)
    } catch {
      case ex: Throwable => ("s" + errorMsg)
        throw ex
    }
  }


  def getInitSC(appName: String, master: String = null): SparkContext = {
    val sconf = new SparkConf().setAppName(appName)
    if (sconf.get("spark.master", null) == null && master == null) sconf.set("spark.master", "local[6]")
    if (master != null) sconf.setMaster(master)

    new SparkContext(sconf)
  }


}
