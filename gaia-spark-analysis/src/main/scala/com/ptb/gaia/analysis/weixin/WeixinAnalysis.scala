package com.ptb.gaia.analysis.weixin

import java.text.SimpleDateFormat
import java.util.{Locale, Calendar}
import java.util
import com.ptb.gaia.service.service.{GWxMediaService}
import com.ptb.gaia.utils.{DataUtil, SetUpUtil}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.bson.Document
import sun.misc.Signal


case class WxDyanmic(val readnum: Double, val likenum: Double);
/**
  * Created by MengChen on 2016/6/29.
  * 老的跑7天数据的程序 微信
  */

class WeixinAnalysis{


  //weixin pmid,midtotalnum,readnum,likenum
  def DataFrameToRdd(hiveContext: HiveContext, toDayDf: DataFrame, preDf: DataFrame): DataFrame = {

    //处理RDD
    val toDayMongodbJoinRdd = toDayDf.map(x => (x.getAs[String]("pmid"),
      WxDyanmic(if(x.getAs[Long]("readnum") !=0L) x.getAs[Long]("readnum") else 1L,
        if(x.getAs[Long]("likenum") !=0L) x.getAs[Long]("likenum") else 1L)));


    val to14DayMongodbJoinRdd = preDf.map(x => (x.getAs[String]("pmid"), WxDyanmic(
      if(x.getAs[Long]("readnum") !=0L) x.getAs[Long]("readnum") else 1L,
      if(x.getAs[Long]("likenum") !=0L) x.getAs[Long]("likenum") else 1L
      )))

    val JoinRdd = toDayMongodbJoinRdd.leftOuterJoin(to14DayMongodbJoinRdd).map({ case (pmid, (day7, day14)) =>
      val realReadnum = (day7, day14) match {
        case (curWeek, lastWeek) if (day14.nonEmpty && day7.readnum <= 10000 && day14.get.readnum <= 10000) =>
          (9999999.00, Option(-1.00))
        case (curWeek, lastWeek) if lastWeek.isEmpty => (curWeek.readnum, Option.empty)
        case (curWeek, lastWeek) => (curWeek.readnum, Option(lastWeek.get.readnum))
      }

      val reallikeNum = (day7, day14) match {
        case (curWeek, lastWeek) if (day14.nonEmpty && day7.likenum <= 10000 && day14.get.likenum <= 10000) =>
          (9999999.00, Option(-1.00))
        case (curWeek, lastWeek) if lastWeek.isEmpty => (curWeek.likenum, Option.empty)
        case (curWeek, lastWeek) => (curWeek.likenum, Option(lastWeek.get.likenum))
      }

      def calGrowRate(cur: Double, last: Double) = java.lang.Double.valueOf(((cur - last) / last).formatted("%.5f"))
      reallikeNum._2 match {
        case r14 if r14.isEmpty => Row(pmid, -10000000.00, -10000000.00)
        case _ => Row(pmid, calGrowRate(realReadnum._1, realReadnum._2.get),calGrowRate(reallikeNum._1, reallikeNum._2.get))
      }
    })




    val RatioSchema = StructType(Array(StructField("pmid", StringType, true), StructField("readnumratio", DoubleType, true),StructField("likenumratio", DoubleType, true)))

    val RatioDF = hiveContext.createDataFrame(JoinRdd.asInstanceOf[RDD[Row]], RatioSchema)

    RatioDF
  }


}


object WeixinAnalysis {
  def apply() = new WeixinAnalysis

  def WeixinAnalysisProcess(args: Array[String], sc: SparkContext): Unit = {

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

//    println("!!!!!!!!!updateTimeStart :" + updateTimeStart)


//    val updateTimeStart = 2016062806L

    //获取 postTime 的时间区间
    val cal1: Calendar = Calendar.getInstance(Locale.CHINA)

    cal1.add(Calendar.DATE, -1)
    cal1.set(Calendar.HOUR_OF_DAY, 0)
    cal1.set(Calendar.MINUTE, 0)
    cal1.set(Calendar.SECOND, 0)

        val postTimeEnd: Long = java.lang.Long.valueOf(new DataUtil().date2TimeStamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ",Locale.CHINA).format(cal1.getTime)) + "000")

//    println("!!!!!!!!!postTimeEnd :" + postTimeEnd)


    cal1.add(Calendar.DATE, -7)
    cal1.set(Calendar.HOUR_OF_DAY, 0)
    cal1.set(Calendar.MINUTE, 0)
    cal1.set(Calendar.SECOND, 0)

        val postTimeStart: Long = java.lang.Long.valueOf(new DataUtil().date2TimeStamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ",Locale.CHINA).format(cal1.getTime)) + "000")

//    println("!!!!!!!!!postTimeStart :" + postTimeStart)
    //计算环比时间获取 updaetime
    val cal2: Calendar = Calendar.getInstance(Locale.CHINA)
    cal2.add(Calendar.DATE, 0)
    cal2.set(Calendar.HOUR_OF_DAY, 23)
    cal2.set(Calendar.MINUTE, 59)
    cal2.set(Calendar.SECOND, 59)

        val update14TimeEnd: Long = java.lang.Long.valueOf(new SimpleDateFormat("yyyyMMddHH",Locale.CHINA).format(cal2.getTime))
//    val update14TimeEnd = 2016062723L
//    println("!!!!!!!!!update14TimeEnd :" + update14TimeEnd)
    cal2.add(Calendar.DATE, -16)
    cal2.set(Calendar.HOUR_OF_DAY, 0)
    cal2.set(Calendar.MINUTE, 0)
    cal2.set(Calendar.SECOND, 0)

        val update14TimeStart: Long = java.lang.Long.valueOf(new SimpleDateFormat("yyyyMMddHH",Locale.CHINA).format(cal2.getTime))
//    val update14TimeStart=2016062718L
//    println("!!!!!!!!!update14TimeStart :" + update14TimeStart)
    //获取 7-14 天 postTime 的时间区间
    val cal3: Calendar = Calendar.getInstance(Locale.CHINA)

    cal3.add(Calendar.DATE, -8)
    cal3.set(Calendar.HOUR_OF_DAY, 0)
    cal3.set(Calendar.MINUTE, 0)
    cal3.set(Calendar.SECOND, 0)

        val post14TimeEnd: Long = java.lang.Long.valueOf(new DataUtil().date2TimeStamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ",Locale.CHINA).format(cal3.getTime)) + "000")
//    println("!!!!!!!!!post14TimeEnd :" + post14TimeEnd)
    cal3.add(Calendar.DATE, -7)
    cal3.set(Calendar.HOUR_OF_DAY, 0)
    cal3.set(Calendar.MINUTE, 0)
    cal3.set(Calendar.SECOND, 0)

        val post14TimeStart: Long = java.lang.Long.valueOf(new DataUtil().date2TimeStamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ",Locale.CHINA).format(cal3.getTime)) + "000")

//    println("!!!!!!!!!post14TimeStart :" + post14TimeStart)
    hiveSqlContext.sql("use gaia")

    var s = new StringBuffer()
    //weixin Article 近7天的排名排重
//     val weiXinDistinct = s.append("select * from (select pmid,articleurl,commentnum,forwardnum,likenum,isoriginal,posttime,updatetime,row_number()over(partition by articleurl order by updateTime desc) as rank from wbarticle where time_date>="+ updateTimeStart +" and time_date<="+ updateTimeEnd +" and posttime>="+ postTimeStart +" and posttime<="+ postTimeEnd +") a where rank = 1").toString
    val weiXinDistinct = s.append("select * from (select pmid,articleurl,readnum,likenum,isoriginal,position,posttime,updatetime,row_number()over(partition by articleurl order by updateTime desc) as rank from wxarticle where time_date>="+ updateTimeStart +" and time_date<="+ updateTimeEnd +" and posttime>="+ postTimeStart +" and posttime<="+ postTimeEnd +") a where rank = 1").toString
    hiveSqlContext.sql(weiXinDistinct).repartition(4).registerTempTable("weixintemp")
    hiveSqlContext.cacheTable("weixintemp")

//    println("weixin Article 近7天的排名排重 : " +hiveSqlContext.sql(weiXinDistinct).count())




    //pmid 最新文章  Map(最新文章，posttime)
    s = new StringBuffer()
    val weixinLatestArticle = s.append("select pmid,map(\"value\",articleurl,\"time\",posttime) as latestArticle from (select pmid,articleurl,posttime,row_number()over(partition by pmid order by posttime desc) as rank from weixintemp) a where rank=1").toString
    hiveSqlContext.sql(weixinLatestArticle).repartition(4).registerTempTable("weixinLatestArticle")
    hiveSqlContext.cacheTable("weixinLatestArticle")

//    println("pmid 最新文章  Map(最新文章，posttime) : " +hiveSqlContext.sql(weixinLatestArticle).count())


    s = new StringBuffer()
    //weixin Article 7-14天的排名排重
    val weiXin14Distinct = s.append("select * from (select pmid,articleurl,readnum,likenum,isoriginal,position,posttime,updatetime,row_number()over(partition by articleurl order by updateTime desc) as rank from wxarticle where time_date>="+ update14TimeStart +" and time_date<="+ update14TimeEnd +" and posttime>="+ post14TimeStart +" and posttime<="+ post14TimeEnd +") a where rank = 1").toString
    hiveSqlContext.sql(weiXin14Distinct).repartition(4).registerTempTable("weixin14temp")
    hiveSqlContext.cacheTable("weixin14temp")


//    println("weixin Article 7-14天的排名排重 : " +hiveSqlContext.sql(weiXin14Distinct).count())


    s = new StringBuffer()
    //计算 weixin 7天平均数 和 mid 对应文章总数  pmid,midtotalnum,readnum,likenum
    val weiXinSevenAvgNum = s.append("select a.pmid,a.m_a_count as midtotalnum,floor(sum(a.readnum)/a.m_a_count) as readnum,floor(sum(a.likenum)/a.m_a_count) as likenum from (select pmid,articleurl,readnum,likenum,posttime,updatetime,count(1)over(partition by pmid) as m_a_count from weixintemp where position=1) a group by a.pmid,a.m_a_count").toString
    val weiXinSevenAvgNumDf = hiveSqlContext.sql(weiXinSevenAvgNum)
    weiXinSevenAvgNumDf.repartition(4).registerTempTable("weixinSevenAvgNum")
    hiveSqlContext.cacheTable("weixinSevenAvgNum")

//    println("计算 weixin 7天平均数 和 mid 对应文章总数 : " +weiXinSevenAvgNumDf.count())



    s = new StringBuffer()
    //计算 weixin 7-14天平均数 和 mid 对应文章总数  pmid,midtotalnum,readnum,likenum
    val weiXin14AvgNum = s.append("select a.pmid,a.m_a_count as midtotalnum,floor(sum(a.readnum)/a.m_a_count) as readnum,floor(sum(a.likenum)/a.m_a_count) as likenum from (select pmid,articleurl,readnum,likenum,posttime,updatetime,count(1)over(partition by pmid) as m_a_count from weixin14temp where position=1) a group by a.pmid,a.m_a_count").toString
    val weiXin14AvgNumDf = hiveSqlContext.sql(weiXin14AvgNum)
    weiXin14AvgNumDf.repartition(4).registerTempTable("weixin14AvgNum")
    hiveSqlContext.cacheTable("weixin14AvgNum")


//    println("计算 weixin 7-14天平均数 和 mid 对应文章总数 : " +weiXin14AvgNumDf.count())

    s = new StringBuffer()
    //计算 weixin 7天每天的柱型计算  pmid,readnumlist,likenumlist，position
    val weiXinSevenPillarNum = s.append("select pmid,collect_list(readnum) as readnumlist,collect_list(likenum) as likenumlist,position from (select pmid,map(\"value\",floor(readnum/m_a_count),\"time\",concat(unix_timestamp(concat(posttime,'000000'),'yyyyMMddHHmmss'),'000')) as readnum,map(\"value\",floor(likenum/m_a_count),\"time\",concat(unix_timestamp(concat(posttime,'000000'),'yyyyMMddHHmmss'),'000')) as likenum,position,posttime from (select pmid,sum(readnum) as readnum,sum(likenum) as likenum,position,posttime,count(1) as m_a_count from (select pmid,readnum,likenum,position,from_unixtime(substr(posttime,0,10),'yyyyMMdd') as posttime from weixintemp) c group by pmid,position,posttime) e ) f group by pmid,position").toString
    hiveSqlContext.sql(weiXinSevenPillarNum).repartition(4).registerTempTable("weixinSevenPillarNum")
    hiveSqlContext.cacheTable("weixinSevenPillarNum")

//    println("计算 weixin 7天每天的柱型计算 : " +hiveSqlContext.sql(weiXinSevenPillarNum).count())

    s = new StringBuffer()
    // 形成  pmid,firstreadlist,secondreadlist,thirdreadlist
    val weiXinSevenPillarResult = s.append("select nvl(c.pmid,d.pmid) as pmid,nvl(c.firstreadlist,Array(map())) as firstreadlist,nvl(c.secondreadlist,Array(map())) as secondreadlist,nvl(d.readnumlist,Array(map())) as thirdreadlist from (select nvl(a.pmid,b.pmid) as pmid,nvl(a.readnumlist,Array(map())) as firstreadlist,nvl(b.readnumlist,Array(map())) as secondreadlist from (select pmid,readnumlist,position from weixinSevenPillarNum where position=1) a full join (select pmid,readnumlist,position from weixinSevenPillarNum where position=2) b on a.pmid=b.pmid) c full join (select pmid,readnumlist,position from weixinSevenPillarNum where position=3) d on c.pmid=d.pmid").toString
    hiveSqlContext.sql(weiXinSevenPillarResult).repartition(4).registerTempTable("weixinSevenPillarResult")
    hiveSqlContext.cacheTable("weixinSevenPillarResult")

//    println("形成  pmid,firstreadlist,secondreadlist,thirdreadlist : " +hiveSqlContext.sql(weiXinSevenPillarResult).count())

    s = new StringBuffer()
    ////wenxin pmid 有多少个原创文章
    val weiXinOriginalCnt = s.append("select pmid,sum(isoriginal) as wxoriginalcnt from (select pmid,nvl(isoriginal,0) as isoriginal from weixintemp) a group by pmid").toString
    hiveSqlContext.sql(weiXinOriginalCnt).repartition(4).registerTempTable("weixinOriginalCnt")
    hiveSqlContext.cacheTable("weixinOriginalCnt")

//    println("wenxin pmid 有多少个原创文章 : " +hiveSqlContext.sql(weiXinOriginalCnt).count())


    s = new StringBuffer()
    //算出 mid = 的总数
    val weiXinMidCnt = s.append("select pmid,count(1) as midcount from (select pmid,mid,count(1) as count from (select pmid,regexp_extract(articleurl,'(mid=)([0-9]*)(&)',2) as mid from weixintemp) a group by pmid,mid) b group by pmid").toString
    hiveSqlContext.sql(weiXinMidCnt).repartition(4).registerTempTable("weixinMidCnt")
    hiveSqlContext.cacheTable("weixinMidCnt")

//    println("算出 mid = 的总数 : " + hiveSqlContext.sql(weiXinMidCnt).count())


    s = new StringBuffer()
    //阅读数上10万 pmid 对应多少
    val weiXin10WanCnt = s.append("select pmid,sum(tenwanflag) as tenwancount from (select pmid,if(readnum>=100000,1,0) as tenwanflag from weixintemp) a group by pmid").toString
    hiveSqlContext.sql(weiXin10WanCnt).repartition(4).registerTempTable("weixin10WanCnt")
    hiveSqlContext.cacheTable("weixin10WanCnt")

//    println("阅读数上10万 pmid 对应多少 : " + hiveSqlContext.sql(weiXin10WanCnt).count())


    s = new StringBuffer()
    //pmid 总点赞数 对应多少
    val weiXinLikesCnt = s.append("select pmid,sum(likenum) as totalLikeNumInPeroid,count(1) as midcount from weixintemp a group by pmid").toString
    hiveSqlContext.sql(weiXinLikesCnt).repartition(4).registerTempTable("weixinLikesCnt")
    hiveSqlContext.cacheTable("weixinLikesCnt")

//    println("pmid 总点赞数 对应多少 : " + hiveSqlContext.sql(weiXinLikesCnt).count())


    val w = WeixinAnalysis()

    //weixin  计算环比 pmid,commentnum,forwardnum,likes
    w.DataFrameToRdd(hiveSqlContext,weiXinSevenAvgNumDf,weiXin14AvgNumDf).repartition(4).registerTempTable("weixinRatio")
    hiveSqlContext.cacheTable("weixinRatio")

//    println("weixin  计算环比 : " + w.DataFrameToRdd(hiveSqlContext,weiXinSevenAvgNumDf,weiXin14AvgNumDf).count())


    //按照环比值排名
    val weiXinRatioSort = hiveSqlContext.sql("select pmid,readnumratio,row_number()over(order by readnumratio desc) readnumrank,likenumratio,row_number()over(order by likenumratio desc) likenumrank from weixinRatio")
    weiXinRatioSort.repartition(4).registerTempTable("weixinRatioSort")
    hiveSqlContext.cacheTable("weixinRatioSort")

//    println("按照环比值排名 : " + weiXinRatioSort.count())


    //合并所有结果
//    val weixinResult = "select j.pmid,j.pubNumInPeroid,j.avgHeadReadNumInPeroid,j.avgHeadLikeNumInPeroid,j.headReadPoints,j.secondReadPoints,j.thirdReadPoints,j.pubOriginalArticleNumInPeroid,j.avgHeadReadRateInPeroid,j.avgHeadLikeRateInPeroid,j.avgHeadReadRateRankInPeroid,j.avgHeadLikeRateRankInPeroid,j.pubArticleNumInPeroid,j.hotArticleNumInPeroid,j.totalLikeNumInPeroid,j.updateTime,k.latestArticle from (select z.pmid,z.pubNumInPeroid,z.avgHeadReadNumInPeroid,z.avgHeadLikeNumInPeroid,z.headReadPoints,z.secondReadPoints,z.thirdReadPoints,z.pubOriginalArticleNumInPeroid,z.avgHeadReadRateInPeroid,z.avgHeadLikeRateInPeroid,z.avgHeadReadRateRankInPeroid,z.avgHeadLikeRateRankInPeroid,y.midcount as pubArticleNumInPeroid,z.hotArticleNumInPeroid,y.totalLikeNumInPeroid,concat(unix_timestamp(),'000') as updateTime from (select k.pmid,k.pubNumInPeroid,k.avgHeadReadNumInPeroid,k.avgHeadLikeNumInPeroid,k.headReadPoints,k.secondReadPoints,k.thirdReadPoints,k.pubOriginalArticleNumInPeroid,k.avgHeadReadRateInPeroid,k.avgHeadLikeRateInPeroid,k.avgHeadReadRateRankInPeroid,k.avgHeadLikeRateRankInPeroid,p.tenwancount as hotArticleNumInPeroid from (select g.pmid,g.pubNumInPeroid,g.avgHeadReadNumInPeroid,g.avgHeadLikeNumInPeroid,g.headReadPoints,g.secondReadPoints,g.thirdReadPoints,g.pubOriginalArticleNumInPeroid,g.avgHeadReadRateInPeroid,g.avgHeadLikeRateInPeroid,g.avgHeadReadRateRankInPeroid,g.avgHeadLikeRateRankInPeroid from (select e.pmid,e.pubNumInPeroid,e.avgHeadReadNumInPeroid,e.avgHeadLikeNumInPeroid,e.headReadPoints,e.secondReadPoints,e.thirdReadPoints,e.pubOriginalArticleNumInPeroid,f.readnumratio as avgHeadReadRateInPeroid,f.likenumratio as avgHeadLikeRateInPeroid,f.readnumrank as avgHeadReadRateRankInPeroid,f.likenumrank as avgHeadLikeRateRankInPeroid from (select c.pmid,c.pubNumInPeroid,c.avgHeadReadNumInPeroid,c.avgHeadLikeNumInPeroid,c.headReadPoints,c.secondReadPoints,c.thirdReadPoints,d.wxoriginalcnt as pubOriginalArticleNumInPeroid from (select a.pmid,a.midtotalnum as pubNumInPeroid,a.readnum as avgHeadReadNumInPeroid,a.likenum as avgHeadLikeNumInPeroid,b.firstreadlist as headReadPoints,b.secondreadlist as secondReadPoints,b.thirdreadlist as thirdReadPoints from weixinSevenAvgNum a join weixinSevenPillarResult b on a.pmid=b.pmid) c join weixinOriginalCnt d on c.pmid=d.pmid) e join weixinRatioSort f on e.pmid=f.pmid) g join weixinMidCnt h on g.pmid=h.pmid) k join weixin10WanCnt p on k.pmid=p.pmid) z join weixinLikesCnt y on z.pmid=y.pmid) j join weixinLatestArticle k on j.pmid=k.pmid"
    val weixinResult = "select nvl(j.pmid,k.pmid) as pmid,nvl(j.pubNumInPeroid,0) as pubNumInPeroid,nvl(j.avgHeadReadNumInPeroid,0) as avgHeadReadNumInPeroid,nvl(j.avgHeadLikeNumInPeroid,0) as avgHeadLikeNumInPeroid,nvl(j.headReadPoints,Array(map())) as headReadPoints,nvl(j.secondReadPoints,Array(map())) as secondReadPoints,nvl(j.thirdReadPoints,Array(map())) as thirdReadPoints,nvl(j.pubOriginalArticleNumInPeroid,0) as pubOriginalArticleNumInPeroid,nvl(j.avgHeadReadRateInPeroid,-1000000) as avgHeadReadRateInPeroid,nvl(j.avgHeadLikeRateInPeroid,-1000000) as avgHeadLikeRateInPeroid,nvl(j.avgHeadReadRateRankInPeroid,1000000) as avgHeadReadRateRankInPeroid,nvl(j.avgHeadLikeRateRankInPeroid,1000000) as avgHeadLikeRateRankInPeroid,nvl(j.pubArticleNumInPeroid,0) as pubArticleNumInPeroid,nvl(j.hotArticleNumInPeroid,0) as hotArticleNumInPeroid,nvl(j.totalLikeNumInPeroid,0) as totalLikeNumInPeroid,nvl(j.updateTime,concat(unix_timestamp(),'000')) as updateTime,nvl(k.latestArticle,map()) as latestArticle from (select nvl(z.pmid,y.pmid) as pmid,nvl(z.pubNumInPeroid,0) as pubNumInPeroid,nvl(z.avgHeadReadNumInPeroid,0) as avgHeadReadNumInPeroid,nvl(z.avgHeadLikeNumInPeroid,0) as avgHeadLikeNumInPeroid,nvl(z.headReadPoints,Array(map())) as headReadPoints,nvl(z.secondReadPoints,Array(map())) as secondReadPoints,nvl(z.thirdReadPoints,Array(map())) as thirdReadPoints,nvl(z.pubOriginalArticleNumInPeroid,0) as pubOriginalArticleNumInPeroid,nvl(z.avgHeadReadRateInPeroid,-1000000) as avgHeadReadRateInPeroid,nvl(z.avgHeadLikeRateInPeroid,-1000000) as avgHeadLikeRateInPeroid,nvl(z.avgHeadReadRateRankInPeroid,1000000) as avgHeadReadRateRankInPeroid,nvl(z.avgHeadLikeRateRankInPeroid,1000000) as avgHeadLikeRateRankInPeroid,nvl(y.midcount,0) as pubArticleNumInPeroid,nvl(z.hotArticleNumInPeroid,0) as hotArticleNumInPeroid,nvl(y.totalLikeNumInPeroid,0) as totalLikeNumInPeroid,concat(unix_timestamp(),'000') as updateTime from (select nvl(k.pmid,p.pmid) as pmid,nvl(k.pubNumInPeroid,0) as pubNumInPeroid,nvl(k.avgHeadReadNumInPeroid,0) as avgHeadReadNumInPeroid,nvl(k.avgHeadLikeNumInPeroid,0) as avgHeadLikeNumInPeroid,nvl(k.headReadPoints,Array(map())) as headReadPoints,nvl(k.secondReadPoints,Array(map())) as secondReadPoints,nvl(k.thirdReadPoints,Array(map())) as thirdReadPoints,nvl(k.pubOriginalArticleNumInPeroid,0) as pubOriginalArticleNumInPeroid,nvl(k.avgHeadReadRateInPeroid,-1000000) as avgHeadReadRateInPeroid,nvl(k.avgHeadLikeRateInPeroid,-1000000) as avgHeadLikeRateInPeroid,nvl(k.avgHeadReadRateRankInPeroid,1000000) as avgHeadReadRateRankInPeroid,nvl(k.avgHeadLikeRateRankInPeroid,1000000) as avgHeadLikeRateRankInPeroid,nvl(p.tenwancount,0) as hotArticleNumInPeroid from (select nvl(g.pmid,h.pmid) as pmid,nvl(h.midcount,0) as pubNumInPeroid,nvl(g.avgHeadReadNumInPeroid,0) as avgHeadReadNumInPeroid,nvl(g.avgHeadLikeNumInPeroid,0) as avgHeadLikeNumInPeroid,nvl(g.headReadPoints,Array(map())) as headReadPoints,nvl(g.secondReadPoints,Array(map())) as secondReadPoints,nvl(g.thirdReadPoints,Array(map())) as thirdReadPoints,nvl(g.pubOriginalArticleNumInPeroid,0) as pubOriginalArticleNumInPeroid,nvl(g.avgHeadReadRateInPeroid,-1000000) as avgHeadReadRateInPeroid,nvl(g.avgHeadLikeRateInPeroid,-1000000) as avgHeadLikeRateInPeroid,nvl(g.avgHeadReadRateRankInPeroid,1000000) as avgHeadReadRateRankInPeroid,nvl(g.avgHeadLikeRateRankInPeroid,1000000) as avgHeadLikeRateRankInPeroid from (select nvl(e.pmid,f.pmid) as pmid,nvl(e.pubNumInPeroid,0) as pubNumInPeroid,nvl(e.avgHeadReadNumInPeroid,0) as avgHeadReadNumInPeroid,nvl(e.avgHeadLikeNumInPeroid,0) as avgHeadLikeNumInPeroid,nvl(e.headReadPoints,Array(map())) as headReadPoints,nvl(e.secondReadPoints,Array(map())) as secondReadPoints,nvl(e.thirdReadPoints,Array(map())) as thirdReadPoints,nvl(e.pubOriginalArticleNumInPeroid,0) as pubOriginalArticleNumInPeroid,nvl(f.readnumratio,-1000000)as avgHeadReadRateInPeroid,nvl(f.likenumratio,-1000000)as avgHeadLikeRateInPeroid,nvl(f.readnumrank,1000000)as avgHeadReadRateRankInPeroid,nvl(f.likenumrank,1000000) as avgHeadLikeRateRankInPeroid from (select nvl(c.pmid,d.pmid) as pmid,nvl(c.pubNumInPeroid,0) as pubNumInPeroid,nvl(c.avgHeadReadNumInPeroid,0) as avgHeadReadNumInPeroid,nvl(c.avgHeadLikeNumInPeroid,0) as avgHeadLikeNumInPeroid,nvl(c.headReadPoints,Array(map())) as headReadPoints,nvl(c.secondReadPoints,Array(map())) as secondReadPoints,nvl(c.thirdReadPoints,Array(map())) as thirdReadPoints,nvl(d.wxoriginalcnt,0) as pubOriginalArticleNumInPeroid from (select nvl(a.pmid,b.pmid) as pmid,nvl(a.midtotalnum,0) as pubNumInPeroid,nvl(a.readnum,0) as avgHeadReadNumInPeroid,nvl(a.likenum,0) as avgHeadLikeNumInPeroid,nvl(b.firstreadlist,Array(map())) as headReadPoints,nvl(b.secondreadlist,Array(map())) as secondReadPoints,nvl(b.thirdreadlist,Array(map())) as thirdReadPoints from weixinSevenAvgNum a full join weixinSevenPillarResult b on a.pmid=b.pmid) c full join weixinOriginalCnt d on c.pmid=d.pmid) e full join weixinRatioSort f on e.pmid=f.pmid) g full join weixinMidCnt h on g.pmid=h.pmid) k full join weixin10WanCnt p on k.pmid=p.pmid) z full join weixinLikesCnt y on z.pmid=y.pmid) j full join weixinLatestArticle k on j.pmid=k.pmid"

//    println("按照环比值排名 : " + hiveSqlContext.sql(weixinResult).count())

    import hiveSqlContext.implicits._
    import scala.collection.convert.decorateAll._
    hiveSqlContext.sql(weixinResult).map(x => (x.getString(0), x))
      .partitionBy(new HashPartitioner(10))
      .map(x => (x._2.getString(0),x._2.getLong(1),x._2.getLong(2),x._2.getLong(3)
        ,x._2.getList[scala.collection.immutable.Map[String, String]](4).asScala
        ,x._2.getList[scala.collection.immutable.Map[String, String]](5).asScala
        ,x._2.getList[scala.collection.immutable.Map[String, String]](6).asScala
        ,x._2.getLong(7)
        ,x._2.getDouble(8)
        ,x._2.getDouble(9)
        ,x._2.getInt(10)
        ,x._2.getInt(11)
        ,x._2.getLong(12)
        ,x._2.getLong(13)
        ,x._2.getLong(14)
        ,x._2.getString(15)
        ,x._2.getJavaMap[String,String](16).asScala
        )
      )
      .toDF("pmid","pubNumInPeroid","avgHeadReadNumInPeroid","avgHeadLikeNumInPeroid","headReadPoints","secondReadPoints","thirdReadPoints","pubOriginalArticleNumInPeroid","avgHeadReadRateInPeroid","avgHeadLikeRateInPeroid","avgHeadReadRateRankInPeroid","avgHeadLikeRateRankInPeroid","pubArticleNumInPeroid","hotArticleNumInPeroid","totalLikeNumInPeroid","updateTime","latestArticle")
      .mapPartitions(x => {
      import scala.collection.JavaConversions

      val wxMediaSave = new GWxMediaService();
      var list = new util.ArrayList[Document]()
      while (x.hasNext) {
        val document: Document = new Document
        val value = x.next()
        document.put("pmid", value.getString(0))
        document.put("pubNumInPeroid", value.getLong(1))
        document.put("avgHeadReadNumInPeroid", value.getLong(2))
        document.put("avgHeadLikeNumInPeroid", value.getLong(3))
        val listcommentnum = new util.ArrayList[util.Map[String,Long]]()
        var listitr = value.getList(4).iterator()
        while(listitr.hasNext){
          val map = new util.HashMap[String,Long]()
          val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String,String]]
          map.put("value",java.lang.Long.valueOf(Commentmap.getOrElse("value",0).toString))
          map.put("time",java.lang.Long.valueOf(Commentmap.getOrElse("time",0).toString))
          listcommentnum.add(map)
        }
        document.put("headReadPoints", listcommentnum)

        val listforwardnum = new util.ArrayList[util.Map[String,Long]]()
        listitr = value.getList(5).iterator()
        while(listitr.hasNext){
          val map = new util.HashMap[String,Long]()
          val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String,String]]
          map.put("value",java.lang.Long.valueOf(Commentmap.getOrElse("value",0).toString))
          map.put("time",java.lang.Long.valueOf(Commentmap.getOrElse("time",0).toString))
          listforwardnum.add(map)
        }
        document.put("secondReadPoints", listforwardnum)

        val listlikenum = new util.ArrayList[util.Map[String,Long]]()
        listitr = value.getList(6).iterator()
        while(listitr.hasNext){
          val map = new util.HashMap[String,Long]()
          val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String,String]]
          map.put("value",java.lang.Long.valueOf(Commentmap.getOrElse("value",0).toString))
          map.put("time",java.lang.Long.valueOf(Commentmap.getOrElse("time",0).toString))
          listlikenum.add(map)
        }
        document.put("thirdReadPoints", listlikenum)

        document.put("pubOriginalArticleNumInPeroid", value.getLong(7))
        document.put("avgHeadReadRateInPeroid", value.getDouble(8))
        document.put("avgHeadLikeRateInPeroid", value.getDouble(9))
        document.put("avgHeadReadRateRankInPeroid", value.getInt(10))
        document.put("avgHeadLikeRateRankInPeroid", value.getInt(11))
        document.put("pubArticleNumInPeroid", value.getLong(12))
        document.put("hotArticleNumInPeroid", value.getLong(13))
        document.put("totalLikeNumInPeroid", value.getLong(14))
        document.put("updateTime", java.lang.Long.valueOf(value.getString(15)))
        document.put("latestArticle", value.getJavaMap(16))
        list.add(document)

        if(list.size()==1500){

          wxMediaSave.insertWxAnalysisBatch(list)
          list.clear()
          listcommentnum.clear()
          listforwardnum.clear()
          listlikenum.clear()
        }

      }

      if(list.size()!=0){
        wxMediaSave.insertWxAnalysisBatch(list)
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

    new SetUpUtil().runJob(WeixinAnalysisProcess, args, new SetUpUtil().getInitSC("WeixinAnalysis"))

  }

}
