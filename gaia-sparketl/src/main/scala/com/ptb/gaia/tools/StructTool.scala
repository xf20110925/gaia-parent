package com.ptb.gaia.tools

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
;

/**
  * Created by eric on 16/7/19.
  */
object StructTool {
  def MongodbToHive(args: Array[String], sc: SparkContext): Unit = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val hiveContext = new HiveContext(sc);
    val k = sqlContext.jsonFile("/Users/eric/wk/company/ptb/git/gaia-parent/gaia-sparketl/data/wbMedia.json")
    k.saveAsParquetFile("/Users/eric/wk/company/ptb/git/gaia-parent/gaia-sparketl/data/output")
  }

  def main(args: Array[String]) {
    new Tools().runJob(MongodbToHive, args, new Tools().getInitSC("jsonTest"))
  }
}
