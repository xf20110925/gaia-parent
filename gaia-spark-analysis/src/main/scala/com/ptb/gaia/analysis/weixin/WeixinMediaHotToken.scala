package com.ptb.gaia.analysis.weixin

import java.text.SimpleDateFormat
import java.util
import java.util.{Locale, Calendar}

import com.ptb.gaia.service.service.GWxMediaService
import com.ptb.gaia.service.utils.ToolsUtil
import com.ptb.gaia.utils.{ToolUtil, DataUtil, SetUpUtil}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.IntegerType
import org.bson.Document
import sun.misc.Signal

/**
  * Created by MengChen on 2016/7/20.
  * 微信热词已经不再线上开始跑了
  */
class WeixinMediaHotToken {


}


object WeixinMediaHotToken {

  def apply() = new WeixinMediaHotToken

  def WeixinMediaHotTokenProcess(args: Array[String], sc: SparkContext): Unit = {

    val hiveSqlContext = new HiveContext(sc)

    //获取 updateTime 时间 开始和结束时间
    val cal: Calendar = Calendar.getInstance(Locale.CHINA)
    cal.add(Calendar.DATE, 0)
    cal.set(Calendar.HOUR_OF_DAY, 23)
    cal.set(Calendar.MINUTE, 59)
    cal.set(Calendar.SECOND, 59)

    val updateTimeEnd: Long = java.lang.Long.valueOf(new SimpleDateFormat("yyyyMMddHH", Locale.CHINA).format(cal.getTime))
    //    val updateTimeEnd = 2016062813L


    cal.add(Calendar.DATE, -16)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)

    val updateTimeStart: Long = java.lang.Long.valueOf(new SimpleDateFormat("yyyyMMddHH", Locale.CHINA).format(cal.getTime))

    //    val updateTimeStart = 2016062806L

    //获取 postTime 的时间区间
    val cal1: Calendar = Calendar.getInstance(Locale.CHINA)

    cal1.add(Calendar.DATE, -1)
    cal1.set(Calendar.HOUR_OF_DAY, 0)
    cal1.set(Calendar.MINUTE, 0)
    cal1.set(Calendar.SECOND, 0)

    val postTimeEnd: Long = java.lang.Long.valueOf(new DataUtil().date2TimeStamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ", Locale.CHINA).format(cal1.getTime)) + "000")

    cal1.add(Calendar.DATE, -14)
    cal1.set(Calendar.HOUR_OF_DAY, 0)
    cal1.set(Calendar.MINUTE, 0)
    cal1.set(Calendar.SECOND, 0)

    val postTimeStart: Long = java.lang.Long.valueOf(new DataUtil().date2TimeStamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ", Locale.CHINA).format(cal1.getTime)) + "000")

    hiveSqlContext.sql("use gaia")
    var s = new StringBuffer()
    //weixin Article 近14天的排名排重
    val weiXinDistinct = s.append("select * from (select pmid,articleurl,hottoken,posttime,updatetime,row_number()over(partition by articleurl order by updateTime desc) as rank from wxarticle where hottoken is not null and time_date>=" + updateTimeStart + " and time_date<=" + updateTimeEnd + " and posttime>=" + postTimeStart + " and posttime<=" + postTimeEnd + ") a where rank = 1").toString
    hiveSqlContext.sql(weiXinDistinct).repartition(8).registerTempTable("weixintemp")
    hiveSqlContext.cacheTable("weixintemp")

    import hiveSqlContext.implicits._
    val tools = new ToolsUtil()
    val scalaTools = ToolUtil()

    s = new StringBuffer()
    //weixin pmid热词排名
    val weiXinHotTokenGroup = s.append("select pmid,token,rank from (select pmid,token,row_number()over(partition by pmid order by cnt) as rank from (select pmid,token,count(1) as cnt from (select pmid,hotword.token as token from weixintemp lateral view explode(hottoken)idtable as hotword) a group by pmid,token) b ) c where rank<=21").toString
    val weiXinHotTokenGroupDf = hiveSqlContext.sql(weiXinHotTokenGroup).repartition(8).rdd.filter(x => tools.includeChinaAndEngChar(x.getString(1)) == false).map(x => (x.getString(0), x.getString(1), x.getInt(2))).toDF("pmid", "token", "rank")
    scalaTools.castColumnTo(weiXinHotTokenGroupDf, "rank", IntegerType).registerTempTable("weiXinHotTokenGroup")
    hiveSqlContext.cacheTable("weiXinHotTokenGroup")


    s = new StringBuffer()
    val weiXinResult = s.append("select pmid,collect_list(HotWords) as mediaHotWords from (select pmid,map(\"token\",token,\"score\",rank) as HotWords from weiXinHotTokenGroup) a group by pmid").toString

    import scala.collection.convert.decorateAll._
    hiveSqlContext.sql(weiXinResult).map(x => (x.getString(0), x))
      .partitionBy(new HashPartitioner(10))
      .map(x => (x._2.getString(0), x._2.getList[scala.collection.immutable.Map[String, String]](1).asScala))
      .toDF("pmid", "mediaHotWords")
      .mapPartitions(x => {

      import scala.collection.convert.decorateAll._

      val wxMediaSave = new GWxMediaService()
      var list = new util.ArrayList[Document]()
      while (x.hasNext) {
        val document: Document = new Document
        val value = x.next()
        document.put("pmid", value.getString(0))

        val listcommentnum = new util.ArrayList[util.Map[String, String]]()
        var listitr = value.getList(1).iterator()
        while (listitr.hasNext) {
          var map: util.Map[String, String] = new util.HashMap[String, String]()
          map = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]].asJava
          listcommentnum.add(map)
        }
        document.put("mediaHotWords", listcommentnum)
        list.add(document)

        if (list.size() == 2000) {
          wxMediaSave.insertWxHotWordBatch(list)
          list.clear()
          listcommentnum.clear()
        }

      }

      if (list.size() != 0) {
        wxMediaSave.insertWxHotWordBatch(list)
        list.clear()
      }

      List().iterator

    }).count()


    hiveSqlContext.clearCache()
    Signal.raise(new Signal("INT"))
  }


  def main(args: Array[String]) {

    new SetUpUtil().runJob(WeixinMediaHotTokenProcess, args, new SetUpUtil().getInitSC("WeixinMediaHotToken"))

  }


}
