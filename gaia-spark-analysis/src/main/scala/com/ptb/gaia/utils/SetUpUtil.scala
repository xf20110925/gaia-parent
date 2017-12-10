package com.ptb.gaia.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MengChen on 2016/5/31.
  */
class SetUpUtil {

  /*
    执行job作业
   */

  def runJob(job: (Array[String], SparkContext) => Unit, args: Array[String],
             context: SparkContext, errorMsg: String = "任务执行失败"): Unit = {
    try {
      job(args, context)
    } catch {
      case ex: Throwable => ("s" + errorMsg)
        throw ex
    }
  }

  /*
  可以初始化参数 返回SparkContext
   */

  def getInitSC(appName: String, master: String = null): SparkContext = {
    val sconf = new SparkConf().setAppName(appName)
    if (sconf.get("spark.master", null) == null && master == null) sconf.set("spark.master", "local[6]")
    if (master != null) sconf.setMaster(master)

    new SparkContext(sconf)

  }
}
