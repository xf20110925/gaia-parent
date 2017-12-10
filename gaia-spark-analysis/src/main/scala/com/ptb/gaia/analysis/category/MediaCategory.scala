package com.ptb.gaia.analysis.category

import com.ptb.gaia.utils.SetUpUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by MengChen on 2016/9/7.
  */
class MediaCategory {

}

object MediaCategory {

  def MediaCategoryProcess(args: Array[String], sc: SparkContext): Unit = {

    val hiveSqlContext = new HiveContext(sc)

    hiveSqlContext.sql("use gaia")

    var s = new StringBuffer()

    val wxLastMedia = s.append("select * from (select *,row_number() over(PARTITION BY pmid order by updateTime) as rank from gaia.wxmedia) a where a.rank=1").toString

    hiveSqlContext.sql(wxLastMedia).repartition(4).registerTempTable("wxLastMedia")
    hiveSqlContext.cacheTable("wxLastMedia")

    s = new StringBuffer()

    val wbLastMedia = s.append("select * from (select *,row_number() over(PARTITION BY pmid order by updateTime) as rank from gaia.wbmedia) a where a.rank=1").toString

    hiveSqlContext.sql(wbLastMedia).repartition(4).registerTempTable("wbLastMedia")
    hiveSqlContext.cacheTable("wbLastMedia")

    s = new StringBuffer()

    val mediaCategory = s.append("select k.*,p.name from (select pmid,medianame,brief,rates,categorys,type from gaia.mediapredicatecategory lateral view explode(category) adtable as categorys) k join gaia.defalut_interest p on k.categorys = p.type_id").toString
    hiveSqlContext.sql(mediaCategory).repartition(4).registerTempTable("mediaCategory")
    hiveSqlContext.cacheTable("mediaCategory")

    hiveSqlContext.sql("drop table gaia.wxcategory")
    hiveSqlContext.sql("drop table gaia.wbcategory")

    s = new StringBuffer()
    val wbResult = s.append("select z.pmid,z.mediaName,z.fansNum,z.postNum,z.pubNumInPeroid,z.avgCommentNumInPeroid,z.avgForwardNumInPeroid,z.avgLikeNumInPeroid,z.totalCommentPoints,z.totalForwardPoints,z.totalLikePoints,z.pubOriginalArticleNumInPeroid,z.avgCommentRateInPeroid,z.avgForwardRateInPeroid,z.avgLikeRateInPeroid,z.avgCommentRateRankInPeroid,z.avgForwardRateRankInPeroid,z.avgLikeRateRankInPeroid,z.latestArticle,z.mediaHotWords,z.brief,z.gender,z.headImage,z.isAuth,z.authInfo,z.location,z.itags,t.name,t.rates,t.categorys,t.type,z.updateTime,z.regiterTime,z.addTime from wbLastMedia z left join mediaCategory t on z.pmid=t.pmid where t.pmid is not null").toString
    hiveSqlContext.sql(wbResult).repartition(8).write.mode(SaveMode.Overwrite).saveAsTable("gaia.wbcategory")

    s = new StringBuffer()
    val wxResult = s.append("select z.pmid,z.mediaName,z.pubNumInPeroid,z.avgHeadReadNumInPeroid,z.avgHeadLikeNumInPeroid,z.headReadPoints,z.secondReadPoints,z.thirdReadPoints,z.pubOriginalArticleNumInPeroid,z.avgHeadReadRateInPeroid,z.avgHeadLikeRateInPeroid,z.avgHeadReadRateRankInPeroid,z.avgHeadLikeRateRankInPeroid,z.pubArticleNumInPeroid,z.hotArticleNumInPeroid,z.totalLikeNumInPeroid,z.latestArticle,z.mediaHotWords,z.authInfo,z.brief,z.headImage,z.isAuth,z.qrcode,z.weixinId,t.name,t.rates,t.categorys,t.type,z.updateTime,z.addTime from wxLastMedia z left join mediaCategory t on z.pmid=t.pmid where t.pmid is not null").toString
    hiveSqlContext.sql(wxResult).repartition(8).write.mode(SaveMode.Overwrite).saveAsTable("gaia.wxcategory")

  }

  def main(args: Array[String]) {

    new SetUpUtil().runJob(MediaCategoryProcess, args, new SetUpUtil().getInitSC("MediaCategory"))

  }
}
