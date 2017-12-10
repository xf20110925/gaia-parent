package com.ptb.gaia.analysis.zeus

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.ptb.gaia.analysis.calculation.MediaChannelDetailHandle
import com.ptb.gaia.analysis.calculation.MediaChannelDetailHandle._
import com.ptb.gaia.utils.SetUpUtil
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.jsoup.Jsoup

/**
  * 用户媒体发布文章导入mysql
  */
object UMAExport {

  def handle(args: Array[String], sc: SparkContext): Unit ={
    val hiveSqlContext = new HiveContext(sc)
    //用户收藏的媒体
    val uMDf = hiveSqlContext.read.jdbc(mysqlUrl, "user_media", new Properties).select("type", "p_mid").distinct().withColumnRenamed("p_mid", "pmid")
    uMDf.cache
    //用户收藏媒体发文
    val uMADf = hiveSqlContext.read.jdbc(mysqlUrl, "media_article", Array("pmid in (select distinct pmid from media_article)"), new Properties).select("plat_type", "pmid").distinct().withColumnRenamed("plat_type", "type")
    uMADf.cache
    //交集
    val oneDayDf = uMDf.intersect(uMADf)
    //差集
    val thirtyDayDf = uMDf.except(uMADf)
    export(oneDayDf, hiveSqlContext, 1*24*3600)
    export(thirtyDayDf, hiveSqlContext, 30*24*3600)
  }

  //导出用户媒体最近一个月发文
  def export(uPMediaDf:DataFrame, hiveSqlContext: HiveContext, time:Long): Unit = {

    val wxMSql = "select pmid, medianame, headimage, if(isauth=1, 1, 0) isauth from (select pmid, medianame, headimage, isauth, row_number() over(PARTITION BY pmid order by updateTime desc) as rank from gaia.wxmedia where time_date > from_unixtime(unix_timestamp(),'yyyyMMdd')-10) a where a.rank=1"
    val wbMSql = "select pmid, medianame, headimage, if(isauth=1, 1, 0) isauth from (select pmid, medianame, headimage, isauth, row_number() over(PARTITION BY pmid order by updateTime desc) as rank from gaia.wbmedia where time_date > from_unixtime(unix_timestamp(),'yyyyMMdd')-10) a where a.rank=1"
    //注册UDF
    hiveSqlContext.udf.register("contentFormate", contentFormate _)
    //微信媒体详细信息
    val wxMDf = hiveSqlContext.sql(wxMSql).join(uPMediaDf.filter(uPMediaDf("type") === 1), Seq("pmid"))
    val wbMDf = hiveSqlContext.sql(wbMSql).join(uPMediaDf.filter(uPMediaDf("type") === 2), Seq("pmid"))
    //微信文章
    val wxASql = s"select t.* from (select articleurl, isoriginal, likenum, readnum, pmid, posttime, title, contentFormate(content) content, coverplan, row_number() over(PARTITION BY articleurl order by readnum desc) as rank  from gaia.wxarticle where posttime > (unix_timestamp()- $time) * 1000) t where rank = 1"
    val wbASql = s"select t.* from (select articleurl, isoriginal, commentnum, likenum, forwardnum, pmid, posttime, contentFormate(content) content, coverplan, articletype, row_number() over(PARTITION BY articleurl order by likenum desc) as rank from gaia.wbarticle where posttime > (unix_timestamp()- $time) * 1000) t where rank = 1"
    val wxADf = hiveSqlContext.sql(wxASql)
    val wbADf = hiveSqlContext.sql(wbASql)
    wxADf.join(wxMDf, Seq("pmid")).registerTempTable("wxArticles")
    wbADf.join(wbMDf, Seq("pmid")).registerTempTable("wbArticles")
    try {
      val wxARetSql = "select pmid, medianame media_name, headimage media_image, isauth media_isauth, title, content, coverplan cover_plan, articleurl url, isoriginal, likenum like_num, readnum read_num, posttime post_time, current_timestamp() add_time, 1 plat_type, 'article' article_type  from wxArticles"
      hiveSqlContext.sql(wxARetSql).write.mode(SaveMode.Append).jdbc(mysqlUrl, "media_article", new Properties)
      val wbARetSql = "select pmid, medianame media_name, headimage media_image, isauth media_isauth, content title, content, coverplan cover_plan, articleurl url, isoriginal, likenum like_num, forwardnum spread_num, commentnum comment_num, posttime post_time, current_timestamp() add_time, 2 plat_type, articletype article_type from wbArticles"
      hiveSqlContext.sql(wbARetSql).write.mode(SaveMode.Append).jdbc(mysqlUrl, "media_article", new Properties)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    dataClean
  }

  //删除重复和过期数据
  def dataClean: Unit = {
    val timeMillons = getTimeMillons(-30)("yyyy-MM-dd")
    val cleanSql1 = s"delete from media_article where post_time < $timeMillons"
    val cleanSql2 = "delete from media_article where id not in (select aid from (select max(id) aid from media_article group by url) a)"
    MediaChannelDetailHandle.exeMysqlCommond(Array(cleanSql1, cleanSql2))
  }

  private def contentFormate(content: String): String = {
    val retContent = Jsoup.parseBodyFragment(content).text();
    if (StringUtils.isEmpty(retContent) || retContent.length < 30) retContent
    else retContent.substring(0, 30)
  }

  /**
    * @param day 天数
    * @return 时间戳
    */
  def getTimeMillons(day: Int)(pattern: String = "yyyy-MM-dd"): Long = {
    val calendar = Calendar.getInstance();
    val sdf = new SimpleDateFormat(pattern)
    calendar.setTime(sdf.parse(sdf.format(new Date)))
    calendar.add(Calendar.DAY_OF_YEAR, day)
    calendar.getTimeInMillis
  }

  def main(args: Array[String]) {
    new SetUpUtil().runJob(handle, args, new SetUpUtil().getInitSC("userMediaArticle"))
  }
}
