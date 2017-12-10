package com.ptb.gaia.analysis.weixin

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}
import java.util

import com.ptb.gaia.common.Constant
import com.ptb.gaia.service.service.GWxMediaService
import com.ptb.gaia.utils.{DataUtil, SetUpUtil}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.bson.Document
import sun.misc.Signal

/*
现在线上正在跑的数据任务 力度为按天统计计算，输出到Hive表 wxanalysis_basic  微信
 */

class WeixinAnalysisBasic {

  def Analysis(hiveSqlContext: HiveContext, updateTimeStart: Long, updateTimeEnd: Long, postTimeStart: Long, postTimeEnd: Long, partition: String): Unit = {

    hiveSqlContext.sql("use gaia")

    var s = new StringBuffer()
    //weixin Article 近7天的排名排重 pmid,articleurl,readnum,likenum,isoriginal,position,posttime,updatetime
    val weiXinDistinct = s.append("select pmid,articleurl,readnum,likenum,isoriginal,position,posttime,updatetime from (select pmid,articleurl,readnum,likenum,isoriginal,position,posttime,updatetime,row_number()over(partition by articleurl order by updateTime desc) as rank from wxarticle where time_date>=" + updateTimeStart + " and time_date<=" + updateTimeEnd + " and posttime>=" + postTimeStart + " and posttime<=" + postTimeEnd + ") a where rank = 1").toString
    hiveSqlContext.sql(weiXinDistinct).repartition(8).registerTempTable("weixintemp")
    hiveSqlContext.cacheTable("weixintemp")

    //pmid 最新文章  Map(最新文章，posttime)  pmid,latestArticle
    s = new StringBuffer()
    val weixinLatestArticle = s.append("select pmid,map(\"value\",articleurl,\"time\",posttime) as latestArticle from (select pmid,articleurl,posttime,row_number()over(partition by pmid order by posttime desc) as rank from weixintemp) a where rank=1").toString
    hiveSqlContext.sql(weixinLatestArticle).repartition(8).registerTempTable("weixinLatestArticle")
    hiveSqlContext.cacheTable("weixinLatestArticle")

    s = new StringBuffer()
    //计算 weixin 7天平均数 和 mid 对应文章总数  pmid,midtotalnum,readnum,likenum
    val weiXinTodayAvgNum = s.append("select a.pmid,a.m_a_count as midtotalnum,floor(sum(a.readnum)/a.m_a_count) as readnum,floor(sum(a.likenum)/a.m_a_count) as likenum from (select pmid,articleurl,readnum,likenum,posttime,updatetime,count(1)over(partition by pmid) as m_a_count from weixintemp where position=1) a group by a.pmid,a.m_a_count").toString
    val weiXinTodayAvgNumDf = hiveSqlContext.sql(weiXinTodayAvgNum)
    weiXinTodayAvgNumDf.repartition(8).registerTempTable("weiXinTodayAvgNum")
    hiveSqlContext.cacheTable("weiXinTodayAvgNum")

    s = new StringBuffer()
    //计算 weixin 7天每天的柱型计算  pmid,readnum,likenum，position,posttime
    val weiXinTodayPillarNum = s.append("SELECT pmid,map(\"value\",floor(readnum/m_a_count),\"time\",concat(unix_timestamp(concat(posttime,'000000'),'yyyyMMddHHmmss'),'000')) AS readnum,map(\"value\",floor(likenum/m_a_count),\"time\",concat(unix_timestamp(concat(posttime,'000000'),'yyyyMMddHHmmss'),'000')) AS likenum,POSITION,posttime FROM (SELECT pmid,sum(readnum) AS readnum,sum(likenum) AS likenum,POSITION,posttime,count(1) AS m_a_count FROM (SELECT pmid,readnum,likenum,POSITION,from_unixtime(substr(posttime,0,10),'yyyyMMdd') AS posttime FROM weixintemp) c GROUP BY pmid,POSITION,posttime) e").toString
    hiveSqlContext.sql(weiXinTodayPillarNum).repartition(8).registerTempTable("weiXinTodayPillarNum")
    hiveSqlContext.cacheTable("weiXinTodayPillarNum")

    s = new StringBuffer()
    // 形成  pmid,firstreadlist,secondreadlist,thirdreadlist
    val weiXinTodayPillarResult = s.append("SELECT nvl(c.pmid,d.pmid) AS pmid,nvl(c.firstreadlist,map()) AS firstreadlist,nvl(c.secondreadlist,map()) AS secondreadlist,nvl(d.readnum,map()) AS thirdreadlist FROM (SELECT nvl(a.pmid,b.pmid) AS pmid,nvl(a.readnum,map()) AS firstreadlist,nvl(b.readnum,map()) AS secondreadlist FROM (SELECT pmid,readnum,POSITION FROM weiXinTodayPillarNum WHERE POSITION=1) a FULL JOIN (SELECT pmid,readnum,POSITION FROM weiXinTodayPillarNum WHERE POSITION=2) b ON a.pmid=b.pmid) c FULL JOIN (SELECT pmid,readnum,POSITION FROM weiXinTodayPillarNum WHERE POSITION=3) d ON c.pmid=d.pmid").toString
    hiveSqlContext.sql(weiXinTodayPillarResult).repartition(8).registerTempTable("weiXinTodayPillarResult")
    hiveSqlContext.cacheTable("weiXinTodayPillarResult")

    s = new StringBuffer()
    ////wenxin pmid 有多少个原创文章 pmid，wxoriginalcnt
    val weiXinOriginalCnt = s.append("select pmid,sum(isoriginal) as wxoriginalcnt from (select pmid,nvl(isoriginal,0) as isoriginal from weixintemp) a group by pmid").toString
    hiveSqlContext.sql(weiXinOriginalCnt).repartition(8).registerTempTable("weixinOriginalCnt")
    hiveSqlContext.cacheTable("weixinOriginalCnt")

    s = new StringBuffer()
    //算出 mid = 的总数  pmid，midcount
    val weiXinMidCnt = s.append("select pmid,count(1) as midcount from (select pmid,mid,count(1) as count from (select pmid,regexp_extract(articleurl,'(mid=)([0-9]*)(&)',2) as mid from weixintemp) a group by pmid,mid) b group by pmid").toString
    hiveSqlContext.sql(weiXinMidCnt).repartition(8).registerTempTable("weixinMidCnt")
    hiveSqlContext.cacheTable("weixinMidCnt")

    s = new StringBuffer()
    //阅读数上10万 pmid 对应多少  pmid,tenwancount
    val weiXin10WanCnt = s.append("select pmid,sum(tenwanflag) as tenwancount from (select pmid,if(readnum>=100000,1,0) as tenwanflag from weixintemp) a group by pmid").toString
    hiveSqlContext.sql(weiXin10WanCnt).repartition(8).registerTempTable("weixin10WanCnt")
    hiveSqlContext.cacheTable("weixin10WanCnt")

    s = new StringBuffer()
    //pmid 总点赞数 对应多少   pmid,totalLikeNumInPeroid,midcount
    val weiXinLikesCnt = s.append("select pmid,sum(likenum) as totalLikeNumInPeroid,count(1) as midcount from weixintemp a group by pmid").toString
    hiveSqlContext.sql(weiXinLikesCnt).repartition(8).registerTempTable("weixinLikesCnt")
    hiveSqlContext.cacheTable("weixinLikesCnt")

    hiveSqlContext.sql("set hive.exec.dynamic.partition=true;")
    hiveSqlContext.sql("set hive.exec.dynamic.partition.mode=nostrick;")

    //合并结果
    val weixinResult = "INSERT overwrite TABLE gaia.wxanalysis_basic partition(time_date) SELECT pmid,pubNumInPeroid,avgHeadReadNumInPeroid,avgHeadLikeNumInPeroid,headReadPoints,secondReadPoints,thirdReadPoints,pubOriginalArticleNumInPeroid,hotArticleNumInPeroid,pubArticleNumInPeroid,totalLikeNumInPeroid,latestArticle,updateTime,posttime as time_date from (select nvl(k.pmid,t.pmid) AS pmid,nvl(k.pubNumInPeroid,0) AS pubNumInPeroid,nvl(k.avgHeadReadNumInPeroid,0) AS avgHeadReadNumInPeroid,nvl(k.avgHeadLikeNumInPeroid,0) AS avgHeadLikeNumInPeroid,nvl(k.headReadPoints,map()) AS headReadPoints,nvl(k.secondReadPoints,map()) AS secondReadPoints,nvl(k.thirdReadPoints,map()) AS thirdReadPoints,nvl(k.pubOriginalArticleNumInPeroid,0) AS pubOriginalArticleNumInPeroid,nvl(k.hotArticleNumInPeroid,0) AS hotArticleNumInPeroid,nvl(t.midcount,0) AS pubArticleNumInPeroid,nvl(t.totalLikeNumInPeroid,0) AS totalLikeNumInPeroid,nvl(k.latestArticle,map()) AS latestArticle,concat(unix_timestamp(),'000') AS updateTime," + partition + " as posttime from (select nvl(i.pmid,j.pmid) AS pmid,nvl(i.latestArticle,map()) AS latestArticle,nvl(i.pubNumInPeroid,0) AS pubNumInPeroid,nvl(i.avgHeadReadNumInPeroid,0) AS avgHeadReadNumInPeroid,nvl(i.avgHeadLikeNumInPeroid,0) AS avgHeadLikeNumInPeroid,nvl(i.headReadPoints,map()) AS headReadPoints,nvl(i.secondReadPoints,map()) AS secondReadPoints,nvl(i.thirdReadPoints,map()) AS thirdReadPoints,nvl(i.pubOriginalArticleNumInPeroid,0) AS pubOriginalArticleNumInPeroid,nvl(j.tenwancount,0) AS hotArticleNumInPeroid from (select nvl(g.pmid,h.pmid) AS pmid,nvl(g.latestArticle,map()) AS latestArticle,nvl(g.avgHeadReadNumInPeroid,0) AS avgHeadReadNumInPeroid,nvl(g.avgHeadLikeNumInPeroid,0) AS avgHeadLikeNumInPeroid,nvl(g.headReadPoints,map()) AS headReadPoints,nvl(g.secondReadPoints,map()) AS secondReadPoints,nvl(g.thirdReadPoints,map()) AS thirdReadPoints,nvl(g.pubOriginalArticleNumInPeroid,0) AS pubOriginalArticleNumInPeroid,nvl(h.midcount,0) AS pubNumInPeroid from (select nvl(e.pmid,f.pmid) AS pmid,nvl(e.latestArticle,map()) AS latestArticle,nvl(e.pubNumInPeroid,0) AS pubNumInPeroid,nvl(e.avgHeadReadNumInPeroid,0) AS avgHeadReadNumInPeroid,nvl(e.avgHeadLikeNumInPeroid,0) AS avgHeadLikeNumInPeroid,nvl(e.headReadPoints,map()) AS headReadPoints,nvl(e.secondReadPoints,map()) AS secondReadPoints,nvl(e.thirdReadPoints,map()) AS thirdReadPoints,nvl(f.wxoriginalcnt,0) AS pubOriginalArticleNumInPeroid from (select nvl(c.pmid,d.pmid) AS pmid,nvl(c.latestArticle,map()) AS latestArticle,nvl(c.pubNumInPeroid,0) AS pubNumInPeroid,nvl(c.avgHeadReadNumInPeroid,0) AS avgHeadReadNumInPeroid,nvl(c.avgHeadLikeNumInPeroid,0) AS avgHeadLikeNumInPeroid,nvl(d.firstreadlist,map()) AS headReadPoints,nvl(d.secondreadlist,map()) AS secondReadPoints,nvl(d.thirdreadlist,map()) AS thirdReadPoints from (SELECT nvl(a.pmid,b.pmid) AS pmid,nvl(a.latestArticle,map()) AS latestArticle,nvl(b.midtotalnum,0) AS pubNumInPeroid,nvl(b.readnum,0) AS avgHeadReadNumInPeroid,nvl(b.likenum,0) AS avgHeadLikeNumInPeroid FROM weixinLatestArticle a FULL JOIN weiXinTodayAvgNum b ON a.pmid=b.pmid ) c FULL JOIN weiXinTodayPillarResult d ON c.pmid=d.pmid ) e FULL JOIN weixinOriginalCnt f ON e.pmid=f.pmid ) g FULL JOIN weixinMidCnt h ON g.pmid=h.pmid ) i FULL JOIN weixin10WanCnt j ON i.pmid=j.pmid ) k FULL JOIN weixinLikesCnt t ON k.pmid=t.pmid) p"

    hiveSqlContext.sql(weixinResult)

    hiveSqlContext.clearCache()

  }

}

object WeixinAnalysisBasic {

  def apply() = new WeixinAnalysisBasic

  def WeixinAnalysisProcess(args: Array[String], sc: SparkContext): Unit = {

    val hiveSqlContext = new HiveContext(sc)
    val w = WeixinAnalysisBasic()

    //获取当前天的23点59分59秒
    val cal: Calendar = Calendar.getInstance(Locale.CHINA)
    cal.add(Calendar.DATE, 0)
    cal.set(Calendar.HOUR_OF_DAY, 23)
    cal.set(Calendar.MINUTE, 59)
    cal.set(Calendar.SECOND, 59)

    //获取参数第一个 如果等于0 updatetime就是当前天23点59分59秒  否则为用户填写时间戳
    //    val updateTimeEnd: Long = if (args(0).equals("0")) {
    //      java.lang.Long.valueOf(new SimpleDateFormat("yyyyMMddHH", Locale.CHINA).format(cal.getTime))
    //    } else {
    //      java.lang.Long.valueOf(args(0))
    //    }

    var updateTimeEnd: Long = 0L
    var updateTimeStart: Long = 0L
    var postTimeStart: Long = 0L
    var postTimeEnd: Long = 0L

    args.foreach(x => {

      if (x.contains("-")) {

        //例如： 格式 ：20161215
        val min_time: Integer = java.lang.Integer.valueOf(x.split("-")(0))
        val max_time: Integer = java.lang.Integer.valueOf(x.split("-")(1))

        if (min_time > max_time) {
          throw new Exception("Min and Max Time is error : min_time gt max_time");
        }

        import scala.util.control.Breaks._

        for (i <- min_time.toInt to max_time.toInt) {
          breakable {
            if (new DataUtil().isValidDate(String.valueOf(i))) {
              //2016121500
              updateTimeStart = java.lang.Long.valueOf(String.valueOf(i).concat(Constant.uTime_Suffix_0Time))
              //2016123023
              updateTimeEnd = java.lang.Long.valueOf(new DataUtil().addTime(String.valueOf(i), Constant.update_Add_Fifteen_Day).concat(Constant.uTime_suffix_23Time))
              //20161215 000000
              postTimeStart = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA).parse(String.valueOf(i).concat(Constant.pTime_Suffix_0Time)).getTime
              //20161215 235959
              postTimeEnd = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA).parse(String.valueOf(i).concat(Constant.pTime_suffix_23Time)).getTime

              w.Analysis(hiveSqlContext, updateTimeStart, updateTimeEnd, postTimeStart, postTimeEnd, String.valueOf(i))
            }else{
              break()
            }
          }
        }
      } else {
        if (new DataUtil().isValidDate(x)) {
          //2016121500
          updateTimeStart = java.lang.Long.valueOf(x.concat(Constant.uTime_Suffix_0Time))
          //2016123023
          updateTimeEnd = java.lang.Long.valueOf(new DataUtil().addTime(x, Constant.update_Add_Fifteen_Day).concat(Constant.uTime_suffix_23Time))
          //20161215 000000
          postTimeStart = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA).parse(x.concat(Constant.pTime_Suffix_0Time)).getTime
          //20161215 235959
          postTimeEnd = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA).parse(x.concat(Constant.pTime_suffix_23Time)).getTime

          w.Analysis(hiveSqlContext, updateTimeStart, updateTimeEnd, postTimeStart, postTimeEnd, x)
        }
      }
    })

    Signal.raise(new Signal("INT"))
  }

  def main(args: Array[String]) {
    new SetUpUtil().runJob(WeixinAnalysisProcess, args, new SetUpUtil().getInitSC("WeixinAnalysisBasic"))
  }

}
