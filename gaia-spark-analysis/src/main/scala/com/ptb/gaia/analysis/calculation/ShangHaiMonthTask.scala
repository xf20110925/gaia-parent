package com.ptb.gaia.analysis.calculation

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale, Properties}

import com.ptb.gaia.analysis.calculation.MediaChannelDetailHandle.mysqlUrl
import com.ptb.gaia.common.Constant
import com.ptb.gaia.utils.{DataUtil, SetUpUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import sun.misc.Signal

/**
  * Created by MengChen on 2017/3/23.
  */
class ShangHaiMonthTask {

}


object ShangHaiMonthTask {

  def entry(args: Array[String], sc: SparkContext): Unit = {

    val hsc = new HiveContext(sc)

    val Array(interval) = args

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd", Locale.CHINA)

    val time_End = Integer.valueOf(dateFormat.format(new Date()).concat(Constant.uTime_suffix_23Time))
    val time_Start: Integer = Integer.valueOf(new DataUtil().addTime(String.valueOf(time_End).substring(0, 8), -interval.toInt).concat(Constant.uTime_Suffix_0Time))

    val postTimeStart = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA).parse(String.valueOf(time_Start).concat(Constant.pTime_Suffix_0Time)).getTime
    val postTimeEnd = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA).parse(String.valueOf(time_End).concat(Constant.pTime_suffix_23Time)).getTime

    hsc.sql("use gaia")

    val tableQuery = "(select t1.mcid,t.name,t1.pmid from zeus.media_channel t join zeus.media_channel_detail t1 on t.id=t1.mcid where t.plat_type=2 and t.i_id like'%000000' ) weibosort "
    val jdbcDFWeiBo = hsc.read.format("jdbc").options(Map("url" -> mysqlUrl, "dbtable" -> tableQuery)).load()

    val tableQuery2 = "(select t1.mcid,t.name,t1.pmid from zeus.media_channel t join zeus.media_channel_detail t1 on t.id=t1.mcid where t.plat_type=1 and t.`name`=\"网红·主播\" ) weixinsort "
    val jdbcDFWeiXin = hsc.read.format("jdbc").options(Map("url" -> mysqlUrl, "dbtable" -> tableQuery2)).load()

    val tableQuery3 = "(select t.id,t.name,t.i_id from zeus.media_channel t where t.i_id like'%000000' and t.i_id<1500000000 ) media_channel "
    val media_channel = hsc.read.format("jdbc").options(Map("url" -> mysqlUrl, "dbtable" -> tableQuery3)).load()

    val tableQuery4 = "(select t1.mcid,t.name,t1.pmid from zeus.media_channel t join zeus.media_channel_detail t1 on t.id=t1.mcid where t.plat_type=1 and t.i_id like'%000000' and t.i_id<1500000000 ) weixintemp "
    val jdbcDFWeiXin1 = hsc.read.format("jdbc").options(Map("url" -> mysqlUrl, "dbtable" -> tableQuery4)).load()


    jdbcDFWeiBo.registerTempTable("weibosort")
    jdbcDFWeiXin.registerTempTable("weixinsort")
    media_channel.registerTempTable("mediachannel")
    jdbcDFWeiXin1.registerTempTable("weixintemp")

    //basic data
    val basicSql = "SELECT t8.pmid,t8.medianame,if(t8.pi is null,0,t8.pi) AS pi,tags FROM (SELECT t7.pmid,t7.medianame,t7.pi,tags,row_number()over(partition by pmid ORDER BY  t7.pi desc) AS rank FROM gaia.wxmedia t7) t8 WHERE t8.rank=1 "
    hsc.sql(basicSql).write.mode(SaveMode.Overwrite).saveAsTable("gaia.shanghaibasic")


    //weibo 计算
    val sql = "SELECT k.mcid,k.pmid,k.name,m.articleurl,m.commentnum,m.forwardnum,m.likenum,m.isoriginal,m.posttime FROM (SELECT distinct t.mcid,t.name,t.pmid FROM weibosort t ) k JOIN ( SELECT pmid,articleurl,commentnum,forwardnum,likenum,isoriginal,posttime,updatetime FROM (SELECT pmid,articleurl,commentnum,forwardnum,likenum,isoriginal,posttime,updatetime,row_number()over(partition by articleurl ORDER BY  updateTime desc) AS rank FROM gaia.wbarticle WHERE time_date>="+ time_Start +" AND time_date<="+ time_End +" AND posttime>="+ postTimeStart +" AND posttime<="+ postTimeEnd +") a WHERE rank = 1 ) m ON k.pmid=m.pmid "
    hsc.sql(sql).registerTempTable("wbtemp")

    //weibo 计算结果
    val weibosql = "SELECT k.pmid,k.mcid,k.name,k.articleNum,k.totalForwardnum,k.totalCommentnum,k.totalLikenum,k.maxCommentnum,floor(k.totalCommentnum/k.articleNum) AS avgCommentnum,if(z.originalNum is null,0,z.originalNum) AS originalNum FROM (SELECT t.pmid,t.mcid,t.name,count(1) AS articleNum,sum(t.forwardnum) AS totalForwardnum,sum(t.commentnum) AS totalCommentnum,sum(t.likenum) AS totalLikenum,max(t.commentnum) AS maxCommentnum FROM wbtemp t GROUP BY  t.pmid,t.mcid,t.name ) k LEFT JOIN (SELECT p.pmid,count(1) AS originalNum FROM wbtemp p WHERE p.isoriginal=1 GROUP BY  p.pmid ) z ON k.pmid=z.pmid "
    hsc.sql(weibosql).registerTempTable("wbresult1")


    //按照微博总评论数选出top<=200:
    val weiboresult = "select k.name,k.medianame,k.articleNum,k.totalForwardnum,k.avgCommentnum,k.maxCommentnum,k.totalCommentnum,k.totalLikenum,k.originalNum from (select t.mcid,t.name,t.medianame,t.articleNum,t.totalForwardnum,t.avgCommentnum,t.maxCommentnum,t.totalCommentnum,t.totalLikenum,t.originalNum,row_number()over(partition by t.mcid order by t.totalCommentnum desc) as rank from (select t1.mcid,t1.name,t2.medianame,t1.articleNum,t1.totalForwardnum,t1.avgCommentnum,t1.maxCommentnum,t1.totalCommentnum,t1.totalLikenum,t1.originalNum from wbresult1 t1 join (select distinct pmid,medianame from gaia.wbmedia) t2 on t1.pmid=t2.pmid) t ) k join mediachannel p on k.mcid=p.id where k.rank<=200 "
    hsc.sql(weiboresult).write.mode(SaveMode.Overwrite).saveAsTable("gaia.weiboshanghai")

    //weixin
    val weixinsql = "SELECT k.mcid,k.pmid,k.name,m.articleurl,m.readnum,m.likenum,m.isoriginal,m.position,m.posttime FROM ( select distinct t3.mcid,t3.name,t3.pmid from ( SELECT t.mcid,t.name,t.pmid FROM weixintemp t union all SELECT q.mcid,q.name,b.pmid FROM wxmedia b JOIN (SELECT t1.mcid,t2.name,t1.weixinid FROM othertable t1 JOIN mediachannel t2 ON t1.mcid=t2.id) q ON b.weixinid=q.weixinid union all select t10.mcid,t10.name,t10.pmid from weixinsort t10 ) t3 ) k JOIN ( SELECT pmid,articleurl,readnum,likenum,isoriginal,position,posttime,updatetime FROM (SELECT pmid,articleurl,readnum,likenum,isoriginal,position,posttime,updatetime,row_number()over(partition by articleurl ORDER BY  updateTime desc) AS rank FROM wxarticle WHERE time_date>="+ time_Start +" AND time_date<="+ time_End +" AND posttime>="+ postTimeStart +" AND posttime<="+ postTimeEnd +") a WHERE rank = 1 ) m ON k.pmid=m.pmid "
    hsc.sql(weixinsql).registerTempTable("wxtemp")

    //微信文章阅读书 大于10W的文章数
    val wxarticle10w = "select pmid,count(1) as readnumArticle10W from wxtemp t where t.readnum>=100000 group by pmid "
    hsc.sql(wxarticle10w).registerTempTable("wxarticle10w")

    //
    val weixinsql2 = "SELECT k.pmid,k.mcid,k.name,k.articleNum,k.totalReadnum,k.totalLikenum,k.maxReadnum,floor(k.totalReadnum/k.articleNum) AS avgReadnum,if(z.originalNum is null,0,z.originalNum) AS originalNum FROM (SELECT t.pmid,t.mcid,t.name,count(1) AS articleNum,sum(t.readnum) AS totalReadnum,sum(t.likenum) AS totalLikenum,max(t.readnum) AS maxReadnum FROM wxtemp t GROUP BY t.pmid,t.mcid,t.name ) k LEFT JOIN (SELECT p.pmid,count(1) AS originalNum FROM wxtemp p WHERE p.isoriginal=1 GROUP BY p.pmid ) z ON k.pmid=z.pmid "
    hsc.sql(weixinsql2).registerTempTable("wxresult1")

    val weixinresult = "select b.name,b.medianame,b.articleNum,b.totalReadnum,b.totalLikenum,b.maxReadnum,b.avgReadnum,b.originalNum,b.pi,t7.readnumarticle10w from (select k.mcid,k.name,k.pmid,k.medianame,k.articleNum,k.totalReadnum,k.totalLikenum,k.maxReadnum,k.avgReadnum,k.originalNum,k.pi,k.rank from (select t.mcid,t.name,t.pmid,t.medianame,t.articleNum,t.totalReadnum,t.totalLikenum,t.maxReadnum,t.avgReadnum,t.originalNum,t.pi,row_number()over(partition by t.mcid order by t.totalReadnum desc) as rank from (select t1.mcid,t1.name,t2.pmid,t2.medianame,t1.articleNum,t1.totalReadnum,t1.totalLikenum,t1.maxReadnum,t1.avgReadnum,t1.originalNum,t2.pi from wxresult1 t1 join gaia.shanghaibasic t2 on t1.pmid=t2.pmid) t ) k join mediachannel p on k.mcid=p.id where k.rank<=200 ) b join wxarticle10w t7 on b.pmid=t7.pmid "
    hsc.sql(weixinresult).write.mode(SaveMode.Overwrite).saveAsTable("gaia.weixinshanghai")

    hsc.clearCache()
    sc.stop()
  }


  def main(args: Array[String]) {
    new SetUpUtil().runJob(entry, args, new SetUpUtil().getInitSC("ShangHaiMonthTask"))
  }


}
