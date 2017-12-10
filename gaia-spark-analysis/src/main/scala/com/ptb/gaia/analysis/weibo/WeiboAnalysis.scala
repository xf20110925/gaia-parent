package com.ptb.gaia.analysis.weibo

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Locale}

import com.ptb.gaia.service.service.GWbMediaService
import com.ptb.gaia.utils.{DataUtil, SetUpUtil}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.bson.Document
import sun.misc.Signal


case class WbDyanmic(val comment: Double, val forwardnum: Double, val likenum: Double);

/**
  * Created by MengChen on 2016/6/29.
  * 老的跑7天数据的程序  微博
  */
class WeiboAnalysis {

  //计算 weibo 7天平均数 和 mid 对应文章总数  pmid,midtotalnum,commentnum,forwardnum,likenum
  //计算 weibo 7-14天平均数 和 mid 对应文章总数  pmid,midtotalnum,commentnum,forwardnum,likenum
  def DataFrameToRdd(hiveContext: HiveContext, toDayDf: DataFrame, preDf: DataFrame): DataFrame = {

    //处理RDD
    val toDayMongodbJoinRdd = toDayDf.map(x => (x.getAs[String]("pmid"),
      WbDyanmic(if (x.getAs[Long]("commentnum") != 0L) x.getAs[Long]("commentnum") else 1L,
        if (x.getAs[Long]("forwardnum") != 0L) x.getAs[Long]("forwardnum") else 1L,
        if (x.getAs[Long]("likenum") != 0L) x.getAs[Long]("likenum") else 1L))
    )

    val to14DayMongodbJoinRdd = preDf.map(x => (x.getAs[String]("pmid"), WbDyanmic(
      if (x.getAs[Long]("commentnum") != 0L) x.getAs[Long]("commentnum") else 1L,
      if (x.getAs[Long]("forwardnum") != 0L) x.getAs[Long]("forwardnum") else 1L,
      if (x.getAs[Long]("likenum") != 0L) x.getAs[Long]("likenum") else 1L
    )))

    val JoinRdd = toDayMongodbJoinRdd.leftOuterJoin(to14DayMongodbJoinRdd).map({ case (pmid, (day7, day14)) =>
      val realComment = (day7, day14) match {
        case (curWeek, lastWeek) if (day14.nonEmpty && day7.comment <= 1000 && day14.get.comment <= 1000) =>
          (9999999.00, Option(-1.00))
        case (curWeek, lastWeek) if lastWeek.isEmpty => (curWeek.comment, Option.empty)
        case (curWeek, lastWeek) => (curWeek.comment, Option(lastWeek.get.comment))
      }

      val realforward = (day7, day14) match {
        case (curWeek, lastWeek) if (day14.nonEmpty && day7.forwardnum <= 1000 && day14.get.forwardnum <= 1000) =>
          (9999999.00, Option(-1.00))
        case (curWeek, lastWeek) if lastWeek.isEmpty => (curWeek.forwardnum, Option.empty)
        case (curWeek, lastWeek) => (curWeek.forwardnum, Option(lastWeek.get.forwardnum))
      }

      val reallikeNum = (day7, day14) match {
        case (curWeek, lastWeek) if (day14.nonEmpty && day7.likenum <= 1000 && day14.get.likenum <= 1000) =>
          (9999999.00, Option(-1.00))
        case (curWeek, lastWeek) if lastWeek.isEmpty => (curWeek.likenum, Option.empty)
        case (curWeek, lastWeek) => (curWeek.likenum, Option(lastWeek.get.likenum))
      }

      def calGrowRate(cur: Double, last: Double) = java.lang.Double.valueOf(((cur - last) / last).formatted("%.5f"))
      reallikeNum._2 match {
        case r14 if r14.isEmpty => Row(pmid, -10000000.00, -10000000.00, -10000000.00)
        case _ => Row(pmid, calGrowRate(realComment._1, realComment._2.get), calGrowRate(realforward._1, realforward._2.get), calGrowRate(reallikeNum._1, reallikeNum._2.get))
      }
    }
    )

    val RatioSchema = StructType(Array(StructField("pmid", StringType, true), StructField("commentnumratio", DoubleType, true), StructField("forwardnumratio", DoubleType, true), StructField("likenumratio", DoubleType, true)))

    val RatioDF = hiveContext.createDataFrame(JoinRdd.asInstanceOf[RDD[Row]], RatioSchema)

    RatioDF
  }


}


object WeiboAnalysis {

  def apply() = new WeiboAnalysis

  def WeiboAnalysisProcess(args: Array[String], sc: SparkContext): Unit = {

    val hiveSqlContext = new HiveContext(sc)

    //获取 updateTime 时间 开始和结束时间
    val cal: Calendar = Calendar.getInstance(Locale.CHINA)
    cal.add(Calendar.DATE, 0)
    cal.set(Calendar.HOUR_OF_DAY, 23)
    cal.set(Calendar.MINUTE, 59)
    cal.set(Calendar.SECOND, 59)

    val updateTimeEnd: Long = java.lang.Long.valueOf(new SimpleDateFormat("yyyyMMddHH",Locale.CHINA).format(cal.getTime))
    //    val updateTimeEnd = 2016062813L


    cal.add(Calendar.DATE, -9)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)

    val updateTimeStart: Long = java.lang.Long.valueOf(new SimpleDateFormat("yyyyMMddHH",Locale.CHINA).format(cal.getTime))

    //    val updateTimeStart = 2016062806L

    //获取 postTime 的时间区间
    val cal1: Calendar = Calendar.getInstance(Locale.CHINA)

    cal1.add(Calendar.DATE, -1)
    cal1.set(Calendar.HOUR_OF_DAY, 0)
    cal1.set(Calendar.MINUTE, 0)
    cal1.set(Calendar.SECOND, 0)

    val postTimeEnd: Long = java.lang.Long.valueOf(new DataUtil().date2TimeStamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ",Locale.CHINA).format(cal1.getTime)) + "000")

    cal1.add(Calendar.DATE, -7)
    cal1.set(Calendar.HOUR_OF_DAY, 0)
    cal1.set(Calendar.MINUTE, 0)
    cal1.set(Calendar.SECOND, 0)

    val postTimeStart: Long = java.lang.Long.valueOf(new DataUtil().date2TimeStamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ",Locale.CHINA).format(cal1.getTime)) + "000")


    //计算环比时间获取 updaetime
    val cal2: Calendar = Calendar.getInstance(Locale.CHINA)
    cal2.add(Calendar.DATE, 0)
    cal2.set(Calendar.HOUR_OF_DAY, 23)
    cal2.set(Calendar.MINUTE, 59)
    cal2.set(Calendar.SECOND, 59)

    val update14TimeEnd: Long = java.lang.Long.valueOf(new SimpleDateFormat("yyyyMMddHH",Locale.CHINA).format(cal2.getTime))
    //    val update14TimeEnd = 2016062723L

    cal2.add(Calendar.DATE, -14)
    cal2.set(Calendar.HOUR_OF_DAY, 0)
    cal2.set(Calendar.MINUTE, 0)
    cal2.set(Calendar.SECOND, 0)

    val update14TimeStart: Long = java.lang.Long.valueOf(new SimpleDateFormat("yyyyMMddHH",Locale.CHINA).format(cal2.getTime))
    //    val update14TimeStart=2016062718L

    //获取 7-14 天 postTime 的时间区间
    val cal3: Calendar = Calendar.getInstance(Locale.CHINA)

    cal3.add(Calendar.DATE, -8)
    cal3.set(Calendar.HOUR_OF_DAY, 0)
    cal3.set(Calendar.MINUTE, 0)
    cal3.set(Calendar.SECOND, 0)

    val post14TimeEnd: Long = java.lang.Long.valueOf(new DataUtil().date2TimeStamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ",Locale.CHINA).format(cal3.getTime)) + "000")

    cal3.add(Calendar.DATE, -7)
    cal3.set(Calendar.HOUR_OF_DAY, 0)
    cal3.set(Calendar.MINUTE, 0)
    cal3.set(Calendar.SECOND, 0)

    val post14TimeStart: Long = java.lang.Long.valueOf(new DataUtil().date2TimeStamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ",Locale.CHINA).format(cal3.getTime)) + "000")

    //测试
    //    val a = sc.parallelize(Array(("123",(4.0,5.0)),("456",(9.0,7.0)),("789",(9.0,1.0))))
    //    val b = sc.parallelize(Array(("123",(8.0,5.0)),("789",(10,2.0))))
    //    val e = a.leftOuterJoin(b)

    hiveSqlContext.sql("use gaia")

    var s = new StringBuffer()
    //weibo Article 近7天的排名排重
    val weiBoDistinct = s.append("select * from (select pmid,articleurl,commentnum,forwardnum,likenum,isoriginal,posttime,updatetime,row_number()over(partition by articleurl order by updateTime desc) as rank from wbarticle where time_date>=" + updateTimeStart + " and time_date<=" + updateTimeEnd + " and posttime>=" + postTimeStart + " and posttime<=" + postTimeEnd + ") a where rank = 1").toString
    //    val weiBoDistinct = s.append("select * from (select pmid,articleurl,commentnum,forwardnum,likenum,isoriginal,posttime,updatetime,row_number()over(partition by articleurl order by updateTime desc) as rank from wbarticle where time_date>="+ updateTimeStart +" and time_date<="+ updateTimeEnd +") a where rank = 1").toString

    hiveSqlContext.sql(weiBoDistinct).repartition(4).registerTempTable("weibotemp")
    hiveSqlContext.cacheTable("weibotemp")

    //pmid 最新文章  Map(最新文章，posttime)
    s = new StringBuffer()
    val weiBoLatestArticle = s.append("select pmid,map(\"value\",articleurl,\"time\",posttime) as latestArticle from (select pmid,articleurl,posttime,row_number()over(partition by pmid order by posttime desc) as rank from weibotemp) a where rank=1").toString
    hiveSqlContext.sql(weiBoLatestArticle).repartition(4).registerTempTable("weiboLatestArticle")
    hiveSqlContext.cacheTable("weiboLatestArticle")



    s = new StringBuffer()
    //weibo Article 7-14天的排名排重
    val weiBo14Distinct = s.append("select * from (select pmid,articleurl,commentnum,forwardnum,likenum,isoriginal,posttime,updatetime,row_number()over(partition by articleurl order by updateTime desc) as rank from wbarticle where time_date>=" + update14TimeStart + " and time_date<=" + update14TimeEnd + " and posttime>=" + post14TimeStart + " and posttime<=" + post14TimeEnd + ") a where rank = 1").toString
    hiveSqlContext.sql(weiBo14Distinct).repartition(4).registerTempTable("weibo14temp")
    hiveSqlContext.cacheTable("weibo14temp")



    s = new StringBuffer()
    //计算 weibo 7天平均数 和 mid 对应文章总数  pmid,midtotalnum,commentnum,forwardnum,likenum
    val weiBoSevenAvgNum = s.append("select a.pmid,a.m_a_count as midtotalnum,floor(sum(a.commentnum)/a.m_a_count) as commentnum,floor(sum(a.forwardnum)/a.m_a_count) as forwardnum,floor(sum(a.likenum)/a.m_a_count) as likenum from (select pmid,articleurl,commentnum,forwardnum,likenum,isoriginal,posttime,updatetime,count(1)over(partition by pmid) as m_a_count from weibotemp) a group by a.pmid,a.m_a_count").toString
    val weiBoSevenAvgNumDf = hiveSqlContext.sql(weiBoSevenAvgNum)
    weiBoSevenAvgNumDf.repartition(4).registerTempTable("weiboSevenAvgNum")
    hiveSqlContext.cacheTable("weiboSevenAvgNum")



    s = new StringBuffer()
    //计算 weibo 7-14天平均数 和 mid 对应文章总数  pmid,midtotalnum,commentnum,forwardnum,likenum
    val weiBo14AvgNum = s.append("select a.pmid,a.m_a_count as midtotalnum,floor(sum(a.commentnum)/a.m_a_count) as commentnum,floor(sum(a.forwardnum)/a.m_a_count) as forwardnum,floor(sum(a.likenum)/a.m_a_count) as likenum from (select pmid,articleurl,commentnum,forwardnum,likenum,isoriginal,posttime,updatetime,count(1)over(partition by pmid) as m_a_count from weibo14temp) a group by a.pmid,a.m_a_count").toString
    val weiBo14AvgNumDf = hiveSqlContext.sql(weiBo14AvgNum)
    weiBo14AvgNumDf.repartition(4).registerTempTable("weibo14AvgNum")
    hiveSqlContext.cacheTable("weibo14AvgNum")



    s = new StringBuffer()
    //计算 weibo 7天每天的柱型计算
    //    val weiBoSevenPillarNum = s.append("select pmid,collect_list(map(\"value\",commentnum,\"time\",concat(unix_timestamp(concat(posttime,'000000'),'yyyyMMddHHmmss'),'000'))) as commentnumlist,collect_list(map(\"value\",forwardnum,\"time\",concat(unix_timestamp(concat(posttime,'000000'),'yyyyMMddHHmmss'),'000'))) as forwardnumlist,collect_list(map(\"value\",likenum,\"time\",concat(unix_timestamp(concat(posttime,'000000'),'yyyyMMddHHmmss'),'000'))) as likenumlist,posttime from (select pmid,sum(commentnum) as commentnum,sum(forwardnum) as forwardnum,sum(likenum) as likenum,posttime from (select pmid,commentnum,forwardnum,likenum,from_unixtime(substr(posttime,0,10),'yyyyMMdd') as posttime from weibotemp) c group by pmid,posttime) a group by pmid,posttime").toString
    val weiBoSevenPillarNum = s.append("select pmid,collect_list(commentnum) as commentnumlist,collect_list(forwardnum) as forwardnumlist,collect_list(likenum) as likenumlist from (select pmid,map(\"value\",commentnum,\"time\",concat(unix_timestamp(concat(posttime,'000000'),'yyyyMMddHHmmss'),'000')) as commentnum,map(\"value\",forwardnum,\"time\",concat(unix_timestamp(concat(posttime,'000000'),'yyyyMMddHHmmss'),'000')) as forwardnum,map(\"value\",likenum,\"time\",concat(unix_timestamp(concat(posttime,'000000'),'yyyyMMddHHmmss'),'000')) as likenum,posttime from (select pmid,sum(commentnum) as commentnum,sum(forwardnum) as forwardnum,sum(likenum) as likenum,posttime from (select pmid,commentnum,forwardnum,likenum,from_unixtime(substr(posttime,0,10),'yyyyMMdd') as posttime from weibotemp) c group by pmid,posttime) e ) f group by pmid").toString
    hiveSqlContext.sql(weiBoSevenPillarNum).repartition(4).registerTempTable("weiboSevenPillarNum")
    hiveSqlContext.cacheTable("weiboSevenPillarNum")


    s = new StringBuffer()
    ////微博mid 有多少个原创文章
    val weiBoOriginalCnt = s.append("select pmid,sum(isoriginal) as wboriginalcnt from (select pmid,nvl(isoriginal,0) as isoriginal from weibotemp) a group by pmid").toString
    hiveSqlContext.sql(weiBoOriginalCnt).repartition(4).registerTempTable("weiboOriginalCnt")
    hiveSqlContext.cacheTable("weiboOriginalCnt")


    val w = WeiboAnalysis()

    //weibo  计算环比 pmid,commentnum,forwardnum,likes
    w.DataFrameToRdd(hiveSqlContext, weiBoSevenAvgNumDf, weiBo14AvgNumDf).repartition(4).registerTempTable("weiboRatio")
    hiveSqlContext.cacheTable("weiboRatio")

    //按照环比值排名
    val weiboRatioSort = hiveSqlContext.sql("select pmid,commentnumratio,row_number()over(order by commentnumratio desc) commentnumrank,forwardnumratio,row_number()over(order by forwardnumratio desc) forwardnumrank,likenumratio,row_number()over(order by likenumratio desc) likenumrank from weiboRatio")
    weiboRatioSort.repartition(4).registerTempTable("weiboRatioSort")
    hiveSqlContext.cacheTable("weiboRatioSort")

    //合并所有结果
    val weiboResult = "select nvl(j.pmid,k.pmid) as pmid,nvl(j.pubNumInPeroid,0) as pubNumInPeroid,nvl(j.avgCommentNumInPeroid,0) as avgCommentNumInPeroid,nvl(j.avgForwardNumInPeroid,0) as avgForwardNumInPeroid,nvl(j.avgLikeNumInPeroid,0) as avgLikeNumInPeroid,nvl(j.totalCommentPoints,Array(map())) as totalCommentPoints,nvl(j.totalForwardPoints,Array(map())) as totalForwardPoints,nvl(j.totalLikePoints,Array(map())) as totalLikePoints,nvl(j.pubOriginalArticleNumInPeroid,0) as pubOriginalArticleNumInPeroid,nvl(j.avgCommentRateInPeroid,-1000000) as avgCommentRateInPeroid,nvl(j.avgForwardRateInPeroid,-1000000) as avgForwardRateInPeroid,nvl(j.avgLikeRateInPeroid,-1000000) as avgLikeRateInPeroid,nvl(j.avgCommentRateRankInPeroid,1000000) as avgCommentRateRankInPeroid,nvl(j.avgForwardRateRankInPeroid,1000000) as avgForwardRateRankInPeroid,nvl(j.avgLikeRateRankInPeroid,1000000) as avgLikeRateRankInPeroid,nvl(j.updateTime,concat(unix_timestamp(),'000')) as updateTime,nvl(k.latestArticle,map()) as latestArticle from (select nvl(e.pmid,f.pmid) as pmid,nvl(e.pubNumInPeroid,0) as pubNumInPeroid,nvl(e.avgCommentNumInPeroid,0) as avgCommentNumInPeroid,nvl(e.avgForwardNumInPeroid,0) as avgForwardNumInPeroid,nvl(e.avgLikeNumInPeroid,0) as avgLikeNumInPeroid,nvl(e.totalCommentPoints,Array(map())) as totalCommentPoints,nvl(e.totalForwardPoints,Array(map())) as totalForwardPoints,nvl(e.totalLikePoints,Array(map())) as totalLikePoints,nvl(e.pubOriginalArticleNumInPeroid,0) as pubOriginalArticleNumInPeroid,nvl(f.commentnumratio,-1000000) as avgCommentRateInPeroid,nvl(f.forwardnumratio,-1000000) as avgForwardRateInPeroid,nvl(f.likenumratio,-1000000) as avgLikeRateInPeroid,nvl(f.commentnumrank,1000000) as avgCommentRateRankInPeroid,nvl(f.forwardnumrank,1000000) as avgForwardRateRankInPeroid,nvl(f.likenumrank,1000000) as avgLikeRateRankInPeroid,concat(unix_timestamp(),'000') as updateTime from (select nvl(c.pmid,d.pmid) as pmid,nvl(c.pubNumInPeroid,0) as pubNumInPeroid,nvl(c.avgCommentNumInPeroid,0) as avgCommentNumInPeroid,nvl(c.avgForwardNumInPeroid,0) as avgForwardNumInPeroid,nvl(c.avgLikeNumInPeroid,0) as avgLikeNumInPeroid,nvl(c.totalCommentPoints,Array(map())) as totalCommentPoints,nvl(c.totalForwardPoints,Array(map())) as totalForwardPoints,nvl(c.totalLikePoints,Array(map())) as totalLikePoints,nvl(d.wboriginalcnt,0) as pubOriginalArticleNumInPeroid from (select nvl(a.pmid,b.pmid) as pmid,nvl(a.midtotalnum,0) as pubNumInPeroid,nvl(a.commentnum,0) as avgCommentNumInPeroid,nvl(a.forwardnum,0) as avgForwardNumInPeroid,nvl(a.likenum,0) as avgLikeNumInPeroid,nvl(b.commentnumlist,Array(map())) as totalCommentPoints,nvl(b.forwardnumlist,Array(map())) as totalForwardPoints,nvl(b.likenumlist,Array(map())) as totalLikePoints from weiboSevenAvgNum a full join weiboSevenPillarNum b on a.pmid=b.pmid) c full join weiboOriginalCnt d on c.pmid=d.pmid) e full join weiboRatioSort f on e.pmid=f.pmid) j full join weiboLatestArticle k on j.pmid=k.pmid"

    import hiveSqlContext.implicits._
    import scala.collection.convert.decorateAll._
    hiveSqlContext.sql(weiboResult).map(x => (x.getString(0), x))
      .partitionBy(new HashPartitioner(10))
      .map(x => (x._2.getString(0),x._2.getLong(1),x._2.getLong(2),x._2.getLong(3),x._2.getLong(4)
        ,x._2.getList[scala.collection.immutable.Map[String, String]](5).asScala
        ,x._2.getList[scala.collection.immutable.Map[String, String]](6).asScala
        ,x._2.getList[scala.collection.immutable.Map[String, String]](7).asScala
        ,x._2.getLong(8)
        ,x._2.getDouble(9)
        ,x._2.getDouble(10)
        ,x._2.getDouble(11)
        ,x._2.getInt(12)
        ,x._2.getInt(13)
        ,x._2.getInt(14)
        ,x._2.getString(15)
        ,x._2.getJavaMap[String,String](16).asScala
        )
      )
      .toDF("pmid","pubNumInPeroid","avgCommentNumInPeroid","avgForwardNumInPeroid","avgLikeNumInPeroid","totalCommentPoints","totalForwardPoints","totalLikePoints","pubOriginalArticleNumInPeroid","avgCommentRateInPeroid","avgForwardRateInPeroid","avgLikeRateInPeroid","avgCommentRateRankInPeroid","avgForwardRateRankInPeroid","avgLikeRateRankInPeroid","updateTime","latestArticle")
      .mapPartitions(x => {
      import scala.collection.JavaConversions

      val wbMediaSave = new GWbMediaService();
      var list = new util.ArrayList[Document]()
      while (x.hasNext) {
        val document: Document = new Document
        val value = x.next()
        document.put("pmid", value.getString(0))
        document.put("pubNumInPeroid", value.getLong(1))
        document.put("avgCommentNumInPeroid", value.getLong(2))
        document.put("avgForwardNumInPeroid", value.getLong(3))
        document.put("avgLikeNumInPeroid", value.getLong(4))
        val listcommentnum = new util.ArrayList[util.Map[String, Long]]()
        var listitr = value.getList(5).iterator()
        while (listitr.hasNext) {
          val map = new util.HashMap[String, Long]()
          val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]]
          map.put("value", java.lang.Long.valueOf(Commentmap.getOrElse("value", "0").toString))
          map.put("time", java.lang.Long.valueOf(Commentmap.getOrElse("time", "0").toString))
          listcommentnum.add(map)
        }
        document.put("totalCommentPoints", listcommentnum)

        val listforwardnum = new util.ArrayList[util.Map[String, Long]]()
        listitr = value.getList(6).iterator()
        while (listitr.hasNext) {
          val map = new util.HashMap[String, Long]()
          val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]]
          map.put("value", java.lang.Long.valueOf(Commentmap.getOrElse("value", 0).toString))
          map.put("time", java.lang.Long.valueOf(Commentmap.getOrElse("time", 0).toString))
          listforwardnum.add(map)
        }
        document.put("totalForwardPoints", listforwardnum)

        val listlikenum = new util.ArrayList[util.Map[String, Long]]()
        listitr = value.getList(7).iterator()
        while (listitr.hasNext) {
          val map = new util.HashMap[String, Long]()
          val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]]
          map.put("value", java.lang.Long.valueOf(Commentmap.getOrElse("value", 0).toString))
          map.put("time", java.lang.Long.valueOf(Commentmap.getOrElse("time", 0).toString))
          listlikenum.add(map)
        }
        document.put("totalLikePoints", listlikenum)

        document.put("pubOriginalArticleNumInPeroid", value.getLong(8))
        document.put("avgCommentRateInPeroid", value.getDouble(9))
        document.put("avgForwardRateInPeroid", value.getDouble(10))
        document.put("avgLikeRateInPeroid", value.getDouble(11))
        document.put("avgCommentRateRankInPeroid", value.getInt(12))
        document.put("avgForwardRateRankInPeroid", value.getInt(13))
        document.put("avgLikeRateRankInPeroid", value.getInt(14))
        document.put("updateTime", java.lang.Long.valueOf(value.getString(15)))
        document.put("latestArticle", value.getJavaMap(16))
        list.add(document)
        //
        if (list.size() == 1500) {

          wbMediaSave.insertWbAnalysisBatch(list)
          println("process size : "+list.size())
          list.clear()
          listcommentnum.clear()
          listforwardnum.clear()
          listlikenum.clear()
        }

      }
      if (list.size() != 0) {
        wbMediaSave.insertWbAnalysisBatch(list)
        list.clear()
      }
      //添加方法
      List().iterator
    }
    ).count()

    hiveSqlContext.clearCache()

    Signal.raise(new Signal("INT"))

  }

  def main(args: Array[String]) {

    new SetUpUtil().runJob(WeiboAnalysisProcess, args, new SetUpUtil().getInitSC("WeiboAnalysis"))

  }

}
