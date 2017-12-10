package com.ptb.gaia.analysis.weixin

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util
import java.util.{Calendar, Locale}

import com.ptb.gaia.common.Constant
import com.ptb.gaia.service.service.GWxMediaService
import com.ptb.gaia.utils.{DataUtil, SetUpUtil}
import jodd.typeconverter.Convert
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.bson.Document
import sun.misc.Signal

/**
  * Created by MengChen on 2016/12/14.
  */
class AnalysisWxPi {

  /*
      saveType : 微信:1 微博:0
   */
  def analysisPi(args: Array[String], sc: SparkContext, hsc: HiveContext, updateTimeStart: Long, updateTimeEnd: Long, postTimeStart: Long, postTimeEnd: Long): Unit = {

    val Array(readTotalWeight, readHeadWeight, readMaxWeight, readAvgWeight, zanTotalWeight, zanHeadWeight, zanMaxWeight, zanAvgWeight, endTime, timeCycle) = args

    val update_Time: Long = Calendar.getInstance().getTimeInMillis()

    val df = new DecimalFormat("######0.000")
    val readTotal : Double = readTotalWeight.toDouble
    val readHead :Double = readHeadWeight.toDouble
    val readMax : Double = readMaxWeight.toDouble
    val readAvg : Double = readAvgWeight.toDouble
    val zanTotal :Double = zanTotalWeight.toDouble
    val zanHead :Double = zanHeadWeight.toDouble
    val zanMax : Double =  zanMaxWeight.toDouble
    val zanAvg : Double = zanAvgWeight.toDouble
    val RtotalNum: Double = Math.log(30 * 800008)
    val ZtotalNum: Double = Math.log(30 * 80008)
    val RheadNum: Double = Math.log(30 * 100001)
    val ZheadNum: Double = Math.log(30 * 10001)

    hsc.sql("use gaia")
    val resultSql = "SELECT pmid,if(readnum>=100001,100001,readnum) as readnum,if(likenum>=100001,100001,likenum) as likenum,position,count(1)over(partition by pmid) AS pubNum,max(readnum)over(partition by pmid) as rmax,max(likenum)over(partition by pmid) as zmax FROM (SELECT t.pmid,t.readnum,t.likenum,t.position,row_number()over(partition by pmid,articleurl ORDER BY  updateTime desc) AS rank FROM wxarticle t WHERE t.readnum is NOT null AND t.likenum is NOT NULL AND time_date>=" + updateTimeStart + " AND time_date<=" + updateTimeEnd + " and posttime>=" + postTimeStart + " and posttime<=" + postTimeEnd + " ) p WHERE p.rank=1"
    hsc.sql(resultSql).registerTempTable("WeiXinTemp")
    hsc.cacheTable("WeiXinTemp")

    val WeiXinResult = "SELECT pmid,sum(t.readnum) AS rtotal,sum(t.likenum) AS ztotal,floor(sum(t.readnum)/pubNum) as ravg,floor(sum(t.likenum)/pubNum) as zavg,rmax,zmax FROM WeiXinTemp t GROUP BY pmid,pubNum,rmax,zmax"
    hsc.sql(WeiXinResult).registerTempTable("WeiXinResult")
    hsc.cacheTable("WeiXinResult")

    val WeiXinHeadResult = "SELECT pmid,sum(readnum) AS rhead,sum(likenum) AS zhead FROM WeiXinTemp t WHERE t.position=1 GROUP BY pmid"
    hsc.sql(WeiXinHeadResult).registerTempTable("WeiXinHeadResult")
    hsc.cacheTable("WeiXinHeadResult")

    val joinResult = "select t.pmid,nvl(t.rtotal,0) as rtotal,nvl(p.rhead,0) as rhead,nvl(t.rmax,0) as rmax,nvl(t.ravg,0) as ravg,nvl(t.ztotal,0) as ztotal,nvl(p.zhead,0) as zhead,nvl(t.zmax,0) as zmax,nvl(t.zavg,0) as zavg from WeiXinResult t join WeiXinHeadResult p on t.pmid=p.pmid"

    hsc.sql(joinResult).rdd.foreachPartition(iter => {
      val wxMediaSave = new GWxMediaService()
      var list = new util.ArrayList[Document]()
      iter.foreach(row => {
        val document: Document = new Document
        val pi: Integer = Convert.toInteger(df.format(
          (Math.log(row.getLong(1) + 1) / RtotalNum) * readTotal +
            (Math.log(row.getLong(2) + 1) / RheadNum) * readHead +
            (Math.log(row.getLong(3) + 1) / Math.log(100001)) * readMax +
            (Math.log(row.getLong(4) + 1) / Math.log(100001)) * readAvg +
            (Math.log(row.getLong(5) + 1) / ZtotalNum) * zanTotal +
            (Math.log(row.getLong(6) + 1) / ZheadNum) * zanHead +
            (Math.log(row.getLong(7) + 1) / Math.log(10001)) * zanMax +
            (Math.log(row.getLong(8) + 1) / Math.log(10001)) * zanAvg).toDouble * 1000
        )
        document.put("pmid", row.getString(0))
        document.put("pi", pi)
        document.put("updateTime", update_Time)
        list.add(document)
        if (list.size() == 2000) {
          wxMediaSave.insertWxAnalysisBatch(list)
          list.clear()
        }
      })
      if (list.size() != 0) {
        wxMediaSave.insertWxAnalysisBatch(list)
        list.clear()
      }
    }
    )
    hsc.clearCache()
  }


}

object AnalysisWxPi {

  def apply() = new AnalysisWxPi

  def entry(args: Array[String], sc: SparkContext): Unit = {

    val hiveSqlContext = new HiveContext(sc)
    val w = AnalysisWxPi()

    var updateTimeEnd: Long = 0L
    var updateTimeStart: Long = 0L
    var postTimeStart: Long = 0L
    var postTimeEnd: Long = 0L
    /*

        样本媒体分数：PI
        所有文章总阅读数     Rtotal
        头条文章总阅读数     Rhead
        所有文章最高阅读数 Rmax
        所有文章平均阅读数 Ravg
        所有文章总点赞数     Ztotal
        头条文章总点赞数     Zhead
        所有文章最高点赞数 Zmax
        所有文章平均点赞数 Zavg
        发文次数C
        Ravg= Rtotal/C
        Zavg= Ztotal/C

        time type : 20161215
        timeCycle : 7day or 30day Anying
     */
    val Array(readTotalWeight, readHeadWeight, readMaxWeight, readAvgWeight, zanTotalWeight, zanHeadWeight, zanMaxWeight, zanAvgWeight, endTime, timeCycle) = args

    if (Integer.valueOf(endTime) <= 0) {

      throw new Exception("endTime Time is error : <=0")

    }

    //2016121500
    updateTimeStart = java.lang.Long.valueOf(new DataUtil().addTime(endTime, (0 - (Integer.valueOf(timeCycle) + 2))).concat(Constant.uTime_Suffix_0Time))
    //2016123023
    updateTimeEnd = java.lang.Long.valueOf(endTime.concat(Constant.uTime_suffix_23Time))
    //20161215 000000
    postTimeStart = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA).parse(new DataUtil().addTime(endTime, (0 - Integer.valueOf(timeCycle))).concat(Constant.pTime_Suffix_0Time)).getTime
    //20161215 235959
    postTimeEnd = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA).parse(endTime.concat(Constant.pTime_suffix_23Time)).getTime

    w.analysisPi(args, sc, hiveSqlContext, updateTimeStart, updateTimeEnd, postTimeStart, postTimeEnd)

    Signal.raise(new Signal("INT"))

  }

  def main(args: Array[String]) {
    new SetUpUtil().runJob(entry, args, new SetUpUtil().getInitSC("AnalysisPi"))
  }

}

