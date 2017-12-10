package com.ptb.gaia.analysis.weibo

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Locale}

import com.ptb.gaia.common.Constant
import com.ptb.gaia.service.service.GWbMediaService
import com.ptb.gaia.utils.{DataUtil, SetUpUtil}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.bson.Document
import sun.misc.Signal

import scala.util.control.Breaks._

/**
  * Created by MengChen on 2016/6/29.
  * 现在线上正在跑的数据任务 力度为按天统计计算，输出到Hive表 wbanalysis_basic  微博
  */
class WeiboAnalysisBasic {

  def Analysis(hiveSqlContext: HiveContext, updateTimeStart: Long, updateTimeEnd: Long, postTimeStart: Long, postTimeEnd: Long, partition: String): Unit = {

    hiveSqlContext.sql("use gaia")

    var s = new StringBuffer()
    //weibo Article 当天的排名排重
    val weiBoDistinct = s.append("select pmid,articleurl,commentnum,forwardnum,likenum,isoriginal,posttime,updatetime from (select pmid,articleurl,commentnum,forwardnum,likenum,isoriginal,posttime,updatetime,row_number()over(partition by articleurl order by updateTime desc) as rank from wbarticle where time_date>=" + updateTimeStart + " and time_date<=" + updateTimeEnd + " and posttime>=" + postTimeStart + " and posttime<=" + postTimeEnd + ") a where rank = 1").toString
    hiveSqlContext.sql(weiBoDistinct).repartition(8).registerTempTable("weibotemp1")
    hiveSqlContext.cacheTable("weibotemp1")

    s = new StringBuffer()
    //weibo Article join出微博粉丝数大于10000  之前统计的大约这个数为28万的微博媒体 总共5000W
    val weiBoFans = s.append("select a.pmid,a.articleurl,a.commentnum,a.forwardnum,a.likenum,a.isoriginal,a.posttime,a.updatetime from weibotemp1 a join (SELECT distinct(pmid) AS pmid FROM wbmedia WHERE fansnum>=10000) b on a.pmid=b.pmid").toString
    hiveSqlContext.sql(weiBoFans).repartition(8).registerTempTable("weibotemp")
    hiveSqlContext.cacheTable("weibotemp")

    //pmid 最新文章  Map(最新文章，posttime)  pmid，latestArticle
    s = new StringBuffer()
    val weiBoLatestArticle = s.append("select pmid,map(\"value\",articleurl,\"time\",posttime) as latestArticle from (select pmid,articleurl,posttime,row_number()over(partition by pmid order by posttime desc) as rank from weibotemp) a where rank=1").toString
    hiveSqlContext.sql(weiBoLatestArticle).repartition(8).registerTempTable("weiboLatestArticle")
    hiveSqlContext.cacheTable("weiboLatestArticle")

    s = new StringBuffer()
    //计算 weibo 当天平均数 和 mid 对应文章总数 pmid，midtotalnum，commentnum，forwardnum，likenum
    val weiBoTodayAvgNum = s.append("select a.pmid,a.m_a_count as midtotalnum,floor(sum(a.commentnum)/a.m_a_count) as commentnum,floor(sum(a.forwardnum)/a.m_a_count) as forwardnum,floor(sum(a.likenum)/a.m_a_count) as likenum from (select pmid,articleurl,commentnum,forwardnum,likenum,isoriginal,posttime,updatetime,count(1)over(partition by pmid) as m_a_count from weibotemp) a group by a.pmid,a.m_a_count").toString
    hiveSqlContext.sql(weiBoTodayAvgNum).repartition(8).registerTempTable("weiBoTodayAvgNum")
    hiveSqlContext.cacheTable("weiBoTodayAvgNum")

    s = new StringBuffer()
    //计算 weibo 当天柱型计算 不将原来的map变为list 存每天的map就行  pmid,commentnum,forwardnum,likenum
    val weiBoTodayPillarNum = s.append("SELECT pmid,map(\"value\",commentnum,\"time\",concat(unix_timestamp(concat(posttime,'000000'),'yyyyMMddHHmmss'),'000')) AS commentnum,map(\"value\",forwardnum,\"time\",concat(unix_timestamp(concat(posttime,'000000'),'yyyyMMddHHmmss'),'000')) AS forwardnum,map(\"value\",likenum,\"time\",concat(unix_timestamp(concat(posttime,'000000'),'yyyyMMddHHmmss'),'000')) AS likenum,posttime FROM (SELECT pmid,sum(commentnum) AS commentnum,sum(forwardnum) AS forwardnum,sum(likenum) AS likenum,posttime FROM (SELECT pmid,commentnum,forwardnum,likenum,from_unixtime(substr(posttime,0,10),'yyyyMMdd') AS posttime FROM weibotemp) c GROUP BY pmid,posttime) e").toString
    hiveSqlContext.sql(weiBoTodayPillarNum).repartition(8).registerTempTable("weiBoTodayPillarNum")
    hiveSqlContext.cacheTable("weiBoTodayPillarNum")

    s = new StringBuffer()
    ////微博mid 有多少个原创文章 pmid,wboriginalcnt
    val weiBoOriginalCnt = s.append("select pmid,sum(isoriginal) as wboriginalcnt from (select pmid,nvl(isoriginal,0) as isoriginal from weibotemp) a group by pmid").toString
    hiveSqlContext.sql(weiBoOriginalCnt).repartition(8).registerTempTable("weiboOriginalCnt")
    hiveSqlContext.cacheTable("weiboOriginalCnt")

    hiveSqlContext.sql("set hive.exec.dynamic.partition=true;")
    hiveSqlContext.sql("set hive.exec.dynamic.partition.mode=nostrick;")

    s = new StringBuffer()
    //最后一步 统计数据
    val weiboResult = "INSERT overwrite TABLE gaia.wbanalysis_basic partition(time_date) SELECT pmid,pubNumInPeroid,avgCommentNumInPeroid,avgForwardNumInPeroid,avgLikeNumInPeroid,totalCommentPoints,totalForwardPoints,totalLikePoints,latestArticle,pubOriginalArticleNumInPeroid,updateTime,post_time as time_date FROM (SELECT nvl(e.pmid,f.pmid) AS pmid,nvl(e.pubNumInPeroid,0) AS pubNumInPeroid,nvl(e.avgCommentNumInPeroid,0) AS avgCommentNumInPeroid,nvl(e.avgForwardNumInPeroid,0) AS avgForwardNumInPeroid,nvl(e.avgLikeNumInPeroid,0) AS avgLikeNumInPeroid,nvl(e.totalCommentPoints,map()) AS totalCommentPoints,nvl(e.totalForwardPoints,map()) AS totalForwardPoints,nvl(e.totalLikePoints,map()) AS totalLikePoints,nvl(e.latestArticle,map()) AS latestArticle,nvl(f.wboriginalcnt,0) AS pubOriginalArticleNumInPeroid,concat(unix_timestamp(),'000') AS updateTime," + partition + " as post_time FROM (SELECT nvl(c.pmid,d.pmid) AS pmid,nvl(c.latestArticle,map()) AS latestArticle,nvl(c.pubNumInPeroid,0) AS pubNumInPeroid,nvl(c.avgCommentNumInPeroid,0) AS avgCommentNumInPeroid,nvl(c.avgForwardNumInPeroid,0) AS avgForwardNumInPeroid,nvl(c.avgLikeNumInPeroid,0) AS avgLikeNumInPeroid,nvl(d.commentnum,map()) AS totalCommentPoints,nvl(d.forwardnum,map()) AS totalForwardPoints,nvl(d.likenum,map()) AS totalLikePoints FROM (SELECT nvl(a.pmid,b.pmid) AS pmid,nvl(a.latestArticle,map()) AS latestArticle,nvl(b.midtotalnum,0) AS pubNumInPeroid,nvl(b.commentnum,0) AS avgCommentNumInPeroid,nvl(b.forwardnum,0) AS avgForwardNumInPeroid,nvl(b.likenum,0) AS avgLikeNumInPeroid FROM weiboLatestArticle a FULL JOIN weiBoTodayAvgNum b ON a.pmid=b.pmid) c FULL JOIN weiBoTodayPillarNum d ON c.pmid=d.pmid) e FULL JOIN weiboOriginalCnt f ON e.pmid=f.pmid) t"
    hiveSqlContext.sql(weiboResult)

    hiveSqlContext.clearCache()

  }

}

object WeiboAnalysisBasic {

  def apply() = new WeiboAnalysisBasic

  def WeiboAnalysisProcess(args: Array[String], sc: SparkContext): Unit = {

    val hiveSqlContext = new HiveContext(sc)
    val w = WeiboAnalysisBasic()

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
              //1468845366000 000000
              postTimeStart = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA).parse(String.valueOf(i).concat(Constant.pTime_Suffix_0Time)).getTime
              //1468845366000 235959
              postTimeEnd = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA).parse(String.valueOf(i).concat(Constant.pTime_suffix_23Time)).getTime

              w.Analysis(hiveSqlContext, updateTimeStart, updateTimeEnd, postTimeStart, postTimeEnd, String.valueOf(i))
            } else {
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
          //1468845366000 000000
          postTimeStart = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA).parse(x.concat(Constant.pTime_Suffix_0Time)).getTime
          //1468845366000 235959
          postTimeEnd = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA).parse(x.concat(Constant.pTime_suffix_23Time)).getTime

          w.Analysis(hiveSqlContext, updateTimeStart, updateTimeEnd, postTimeStart, postTimeEnd, x)
        }
      }
    })

    Signal.raise(new Signal("INT"))
  }

  def main(args: Array[String]) {
    new SetUpUtil().runJob(WeiboAnalysisProcess, args, new SetUpUtil().getInitSC("WeiboAnalysisBasic"))
  }

}
