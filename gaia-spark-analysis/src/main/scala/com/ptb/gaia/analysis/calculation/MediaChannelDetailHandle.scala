package com.ptb.gaia.analysis.calculation

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.ptb.gaia.utils.SetUpUtil
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

class MediaChannelDetailHandle( wxASaveTName: String, mysqlUrl: String) {

  //微头条文章处理
  def wxHeadLinesHandle(args: Array[String], sc: SparkContext): Unit = {
    val hiveSqlContext = new HiveContext(sc)
    val wxArticleSql = "select * from (select articleurl, isoriginal, likenum, pmid, posttime, readnum, coverplan, title,  row_number() over(PARTITION BY articleurl order by updateTime desc) as rank from gaia.wxarticle where time_date > from_unixtime(unix_timestamp(),'yyyyMMdd')-10 and  posttime > (unix_timestamp()- 7*24*3600) * 1000 and position = 1) t where t.rank = 1"
    val articleDf = hiveSqlContext.sql(wxArticleSql).drop("rank")
    val mcDf = hiveSqlContext.read.jdbc(mysqlUrl, "media_channel", Array("plat_type = 1"), new Properties()).select("id", "i_id").withColumnRenamed("id", "mcid")
    val mcdDf = hiveSqlContext.read.jdbc(mysqlUrl, "media_channel_detail", new Properties()).select("pmid", "mcid")
    val newMediaDf = mcDf.join(mcdDf, Seq("mcid"))
    val mediaDetailSql = "select * from (select pmid, medianame, headimage, row_number() over(PARTITION BY pmid order by updateTime desc) as rank from gaia.wxmedia where time_date > from_unixtime(unix_timestamp(),'yyyyMMdd')-10 and avgheadreadnuminperoid is not null) a where a.rank=1 "
    val retMediaDf = hiveSqlContext.sql(mediaDetailSql).join(newMediaDf, Seq("pmid")).drop("rank")
    articleDf.join(retMediaDf, Seq("pmid")).write.mode(SaveMode.Overwrite).saveAsTable(s"gaia.$wxASaveTName")
  }

  //微头条保存到mysql
  def article2Mysql(args: Array[String], sc: SparkContext): Unit = {
    val hiveSqlContext = new HiveContext(sc)
    //title, converplan conver_plan,
    val wxASql = s"select i_id iid, pmid, articleurl url, medianame media_name, headimage media_image, isoriginal original, coverplan cover_plan, title, readnum read_num, likenum zan_num, posttime post_time, current_timestamp() add_time, 1 ptype, 1 source, 0 ctype from gaia.$wxASaveTName "
    hiveSqlContext.sql(wxASql).write.mode(SaveMode.Append).jdbc(mysqlUrl, "headlines", new Properties)
    val cleanSql1 = "delete from headlines where date_format(add_time,'%Y-%m-%d') < current_date() and source = 1 "
    val cleanSql2 = "delete from headlines where id not in (select aid from (select max(id) aid from headlines group by iid,url) a) and source=1"
    MediaChannelDetailHandle.exeMysqlCommond(Array(cleanSql1, cleanSql2))
  }

}

object MediaChannelDetailHandle {

  val config = new PropertiesConfiguration("ptb.properties");
  val address = config.getString("spring.datasource.url")
  val account = config.getString("spring.datasource.username")
  val password = config.getString("spring.datasource.password")
  val mysqlUrl = s"$address&user=$account&password=$password"


  def exeMysqlCommond(sqls: Array[String]): Unit = {
    var driverManager: Connection = null
    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      driverManager = DriverManager.getConnection(mysqlUrl, account, password)
      for (sql <- sqls) {
        val ps = driverManager.prepareStatement(sql)
        ps.execute()
      }
    } catch {
      case e: Exception => e.printStackTrace
    } finally {
      if (driverManager != null)
        driverManager.close()
    }
  }

  def mediaCategoryRun(args: Array[String], sc: SparkContext): Unit = {
    val mediaChanneDetail = new MediaChannelDetailHandle("t_article_category", mysqlUrl)
    mediaChanneDetail.wxHeadLinesHandle(args, sc)
    mediaChanneDetail.article2Mysql(args, sc)
  }


  def main(args: Array[String]) {
    new SetUpUtil().runJob(mediaCategoryRun, args, new SetUpUtil().getInitSC("mediaCategoryRun"))
  }
}

