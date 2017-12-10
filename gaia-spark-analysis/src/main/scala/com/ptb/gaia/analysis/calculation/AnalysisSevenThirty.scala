package com.ptb.gaia.analysis.calculation

import java.util
import java.util.{Calendar, Properties}

import scala.collection.JavaConversions
import com.ptb.gaia.common.Constant
import com.ptb.gaia.service.IGaiaImpl
import com.ptb.gaia.service.entity.article.Point
import com.ptb.gaia.service.entity.media.{WbMediaStatistics, WxMediaStatistics}
import com.ptb.gaia.service.service.{GWbMediaService, GWxMediaService}
import com.ptb.gaia.utils.{DataUtil, SetUpUtil}
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DataTypes
import org.bson.Document

/**
  * Created by MengChen on 2016/10/14.
  * 7天和30天的计算 分别存储到Mongo
  */
class AnalysisSevenThirty {

  def doWeiXinAnalysis(sc: SparkContext, hsc: HiveContext, saveType: Integer, Time_Start: Integer, Time_End: Integer, update_Time: Long): Unit = {

    hsc.sql("use gaia")
    val resultSql = "SELECT pmid,sum(a.pubnuminperoid) AS pubnuminperoid,floor(sum(a.avgheadreadnuminperoid)) AS avgheadreadnuminperoid,floor(sum(a.avgheadlikenuminperoid)) AS avgheadlikenuminperoid,collect_list(a.headreadpoints) AS headreadpoints,collect_list(a.secondreadpoints) AS secondreadpoints,collect_list(a.thirdreadpoints) AS thirdreadpoints,sum(a.puboriginalarticlenuminperoid) AS puboriginalarticlenuminperoid,sum(a.hotarticlenuminperoid) AS hotarticlenuminperoid,sum(a.pubarticlenuminperoid) AS pubarticlenuminperoid,sum(a.totallikenuminperoid) AS totallikenuminperoid,collect_list(a.latestarticle) AS latestarticle FROM (SELECT pmid,pubnuminperoid,avgheadreadnuminperoid,avgheadlikenuminperoid,headreadpoints,secondreadpoints,thirdreadpoints,puboriginalarticlenuminperoid,hotarticlenuminperoid,pubarticlenuminperoid,totallikenuminperoid,FIRST_VALUE(latestarticle)OVER(PARTITION BY pmid ORDER BY latestarticle['time'] DESC) AS latestarticle FROM gaia.wxanalysis_basic where time_date>=" + Time_Start + " and time_date<=" + Time_End + ") a GROUP BY pmid"
    hsc.sql(resultSql).registerTempTable("resultSql")

    //这部分增加的就是为了 算出头条文章的次数 算出 7天平均除以次数 得到头条平均
    val df = hsc.sql("SELECT t1.pmid,t1.pubnuminperoid,if(t2.headcnt is null,0,floor(round(t1.avgheadreadnuminperoid/t2.headcnt))) AS avgheadreadnuminperoid,if(t2.headcnt is null,0,floor(round(t1.avgheadlikenuminperoid/t2.headcnt))) AS avgheadlikenuminperoid,t1.headreadpoints,t1.secondreadpoints,t1.thirdreadpoints,t1.puboriginalarticlenuminperoid,t1.hotarticlenuminperoid,t1.pubarticlenuminperoid,t1.totallikenuminperoid,t1.latestarticle FROM resultSql t1 LEFT JOIN (SELECT pmid,count(1) AS headcnt FROM wxanalysis_basic t WHERE t.avgheadreadnuminperoid!=0 AND time_date>=" + Time_Start + " AND time_date<=" + Time_End + " GROUP BY  t.pmid ) t2 ON t1.pmid=t2.pmid")

    val resultDf = df.withColumn("headReadPoints", df.col("headReadPoints").cast(DataTypes.createArrayType(DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType, true), true)))
      .withColumn("secondReadPoints", df.col("secondReadPoints").cast(DataTypes.createArrayType(DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType, true), true)))
      .withColumn("thirdReadPoints", df.col("thirdReadPoints").cast(DataTypes.createArrayType(DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType, true), true)))

    if (saveType == Constant.seven_Save_Type) {

      resultDf.foreachPartition(iter => {
        val wxMediaSave = new GWxMediaService()
        var list = new util.ArrayList[Document]()
        iter.foreach(row => {
          val document: Document = new Document
          document.put("pmid", row.getString(0))
          document.put("pubNumInPeroid", row.getLong(1))
          document.put("avgHeadReadNumInPeroid", row.getLong(2))
          document.put("avgHeadLikeNumInPeroid", row.getLong(3))
          document.put("headReadPoints", JavaConversions.seqAsJavaList(JavaConversions.asScalaBuffer(row.getList(4).asInstanceOf[util.List[Map[String, Long]]]).toStream.filter(x => (x.nonEmpty && x.get("time") != null)).map(x => JavaConversions.mapAsJavaMap(x)).toList))
          document.put("secondReadPoints", JavaConversions.seqAsJavaList(JavaConversions.asScalaBuffer(row.getList(5).asInstanceOf[util.List[Map[String, Long]]]).toStream.filter(x => (x.nonEmpty && x.get("time") != null)).map(x => JavaConversions.mapAsJavaMap(x)).toList))
          document.put("thirdReadPoints", JavaConversions.seqAsJavaList(JavaConversions.asScalaBuffer(row.getList(6).asInstanceOf[util.List[Map[String, Long]]]).toStream.filter(x => (x.nonEmpty && x.get("time") != null)).map(x => JavaConversions.mapAsJavaMap(x)).toList))
          document.put("pubOriginalArticleNumInPeroid", row.getLong(7))
          document.put("hotArticleNumInPeroid", row.getLong(8))
          document.put("pubArticleNumInPeroid", row.getLong(9))
          document.put("totalLikeNumInPeroid", row.getLong(10))
          document.put("latestArticle", JavaConversions.mapAsJavaMap(row.getList(11).get(0).asInstanceOf[Map[String, String]]))
          //数据从hive更新到mongodb的时间戳 13位
          document.put("updateTime", update_Time)
          list.add(document)
          if (list.size() == 3000) {
            wxMediaSave.insertWxAnalysisBatch(list)
            list.clear()
          }
        })
        if (list.size() != 0) {
          wxMediaSave.insertWxAnalysisBatch(list)
          list.clear()
        }
      })

    } else if (saveType == Constant.thirty_Save_Type) {
      resultDf.foreachPartition(iter => {
        val wxMediaSave = new GWxMediaService()
        var list = new util.ArrayList[Document]()
        val gaiaImpl = new IGaiaImpl()
        iter.foreach(row => {
          val document: Document = new Document
          document.put("pmid", row.getString(0))
          val wxMediaStatistics: WxMediaStatistics = new WxMediaStatistics()
          wxMediaStatistics.setPubNumInPeroid(Integer.valueOf(row.getLong(1).toString))
          wxMediaStatistics.setAvgHeadReadNumInPeroid(Integer.valueOf(row.getLong(2).toString))
          wxMediaStatistics.setAvgHeadLikeNumInPeroid(Integer.valueOf(row.getLong(3).toString))
          wxMediaStatistics.setHeadReadPoints(JavaConversions.seqAsJavaList(JavaConversions.asScalaBuffer(row.getList(4).asInstanceOf[util.List[Map[String, Long]]]).toStream
            .filter(x => (x.nonEmpty && x.get("time") != null)).map(x => new Point(java.lang.Long.valueOf(x.get("time").get.toString), Integer.valueOf(x.get("value").get.toString))).toList))
          wxMediaStatistics.setSecondReadPoints(JavaConversions.seqAsJavaList(JavaConversions.asScalaBuffer(row.getList(5).asInstanceOf[util.List[Map[String, Long]]]).toStream
            .filter(x => (x.nonEmpty && x.get("time") != null)).map(x => new Point(java.lang.Long.valueOf(x.get("time").get.toString), Integer.valueOf(x.get("value").get.toString))).toList))
          wxMediaStatistics.setThirdReadPoints(JavaConversions.seqAsJavaList(JavaConversions.asScalaBuffer(row.getList(6).asInstanceOf[util.List[Map[String, Long]]]).toStream
            .filter(x => (x.nonEmpty && x.get("time") != null)).map(x => new Point(java.lang.Long.valueOf(x.get("time").get.toString), Integer.valueOf(x.get("value").get.toString))).toList))
          wxMediaStatistics.setPubOriginalArticleNumInPeroid(Integer.valueOf(row.getLong(7).toString))
          wxMediaStatistics.setHotArticleNumInPeroid(Integer.valueOf(row.getLong(8).toString))
          wxMediaStatistics.setPubArticleNumInPeroid(Integer.valueOf(row.getLong(9).toString))
          wxMediaStatistics.setTotalLikeNumInPeroid(Integer.valueOf(row.getLong(10).toString))
          val doc = gaiaImpl.getWxDocument(wxMediaStatistics)
          document.put("wxStatisticsIn30Day", doc)
          document.put("latestArticle", JavaConversions.mapAsJavaMap(row.getList(11).get(0).asInstanceOf[Map[String, String]]))
          //数据从hive更新到mongodb的时间戳 13位
          document.put("updateTime", update_Time)
          list.add(document)
          if (list.size() == 3000) {
            wxMediaSave.insertWxAnalysisBatch(list)
            list.clear()
          }
        })

        if (list.size() != 0) {
          wxMediaSave.insertWxAnalysisBatch(list)
          list.clear()
        }
      })
    }

  }

  def doWeiBoAnalysis(sc: SparkContext, hsc: HiveContext, saveType: Integer, Time_Start: Integer, Time_End: Integer, update_Time: Long): Unit = {

    hsc.sql("use gaia")
    val resultSql = "SELECT a.pmid,sum(a.pubnuminperoid) as pubnuminperoid,floor(sum(a.avgcommentnuminperoid/" + saveType + ")) as avgcommentnuminperoid,floor(sum(a.avgforwardnuminperoid/" + saveType + ")) as avgforwardnuminperoid,floor(sum(a.avglikenuminperoid/" + saveType + ")) as avglikenuminperoid,collect_list(a.totalcommentpoints) as totalcommentpoints,collect_list(a.totalforwardpoints) as totalforwardpoints,collect_list(a.totallikepoints) as totallikepoints,sum(a.puboriginalarticlenuminperoid) as puboriginalarticlenuminperoid,collect_list(a.latestarticle) AS latestarticle FROM (SELECT pmid,pubnuminperoid,avgcommentnuminperoid,avgforwardnuminperoid,avglikenuminperoid,totalcommentpoints,totalforwardpoints,totallikepoints,puboriginalarticlenuminperoid,FIRST_VALUE(latestarticle)OVER(PARTITION BY pmid ORDER BY latestarticle['time'] DESC) AS latestarticle FROM gaia.wbanalysis_basic where time_date>=" + Time_Start + " and time_date<=" + Time_End + ") a GROUP BY pmid"

    val df = hsc.sql(resultSql)

    val resultDf = df.withColumn("totalCommentPoints", df.col("totalCommentPoints").cast(DataTypes.createArrayType(DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType, true), true)))
      .withColumn("totalForwardPoints", df.col("totalForwardPoints").cast(DataTypes.createArrayType(DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType, true), true)))
      .withColumn("totalLikePoints", df.col("totalLikePoints").cast(DataTypes.createArrayType(DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType, true), true)))


    if (saveType == Constant.seven_Save_Type) {
      resultDf.foreachPartition(iter => {
        val wbMediaSave = new GWbMediaService()
        var list = new util.ArrayList[Document]()
        iter.foreach(row => {
          val document: Document = new Document
          document.put("pmid", row.getString(0))
          document.put("pubNumInPeroid", row.getLong(1))
          document.put("avgCommentNumInPeroid", row.getLong(2))
          document.put("avgForwardNumInPeroid", row.getLong(3))
          document.put("avgLikeNumInPeroid", row.getLong(4))
          document.put("totalCommentPoints", JavaConversions.seqAsJavaList(JavaConversions.asScalaBuffer(row.getList(5).asInstanceOf[util.List[Map[String, Long]]]).toStream.filter(x => (x.nonEmpty && x.get("time") != null)).map(x => JavaConversions.mapAsJavaMap(x)).toList))
          document.put("totalForwardPoints", JavaConversions.seqAsJavaList(JavaConversions.asScalaBuffer(row.getList(6).asInstanceOf[util.List[Map[String, Long]]]).toStream.filter(x => (x.nonEmpty && x.get("time") != null)).map(x => JavaConversions.mapAsJavaMap(x)).toList))
          document.put("totalLikePoints", JavaConversions.seqAsJavaList(JavaConversions.asScalaBuffer(row.getList(7).asInstanceOf[util.List[Map[String, Long]]]).toStream.filter(x => (x.nonEmpty && x.get("time") != null)).map(x => JavaConversions.mapAsJavaMap(x)).toList))
          document.put("pubOriginalArticleNumInPeroid", row.getLong(8))
          document.put("latestArticle", JavaConversions.mapAsJavaMap(row.getList(9).get(0).asInstanceOf[Map[String, String]]))
          //数据从hive更新到mongodb的时间戳 13位
          document.put("updateTime", update_Time)
          list.add(document)
          if (list.size() == 3000) {
            wbMediaSave.insertWbAnalysisBatch(list)
            list.clear()
          }
        })
        if (list.size() != 0) {
          wbMediaSave.insertWbAnalysisBatch(list)
          list.clear()
        }
      })

    } else if (saveType == Constant.thirty_Save_Type) {

      resultDf.foreachPartition(iter => {
        val wbMediaSave = new GWbMediaService()
        var list = new util.ArrayList[Document]()
        val gaiaImpl = new IGaiaImpl()
        iter.foreach(row => {
          val document: Document = new Document
          document.put("pmid", row.getString(0))
          val wbMediaStatistics: WbMediaStatistics = new WbMediaStatistics()
          wbMediaStatistics.setPubNumInPeroid(Integer.valueOf(row.getLong(1).toString))
          wbMediaStatistics.setAvgCommentNumInPeroid(Integer.valueOf(row.getLong(2).toString))
          wbMediaStatistics.setAvgForwardNumInPeroid(Integer.valueOf(row.getLong(3).toString))
          wbMediaStatistics.setAvgLikeNumInPeroid(Integer.valueOf(row.getLong(4).toString))
          wbMediaStatistics.setTotalCommentPoints(JavaConversions.seqAsJavaList(JavaConversions.asScalaBuffer(row.getList(5).asInstanceOf[util.List[Map[String, Long]]]).toStream
            .filter(x => (x.nonEmpty && x.get("time") != null)).map(x => new Point(java.lang.Long.valueOf(x.get("time").get.toString), Integer.valueOf(x.get("value").get.toString))).toList))
          wbMediaStatistics.setTotalForwardPoints(JavaConversions.seqAsJavaList(JavaConversions.asScalaBuffer(row.getList(6).asInstanceOf[util.List[Map[String, Long]]]).toStream
            .filter(x => (x.nonEmpty && x.get("time") != null)).map(x => new Point(java.lang.Long.valueOf(x.get("time").get.toString), Integer.valueOf(x.get("value").get.toString))).toList))
          wbMediaStatistics.setTotalLikePoints(JavaConversions.seqAsJavaList(JavaConversions.asScalaBuffer(row.getList(7).asInstanceOf[util.List[Map[String, Long]]]).toStream
            .filter(x => (x.nonEmpty && x.get("time") != null)).map(x => new Point(java.lang.Long.valueOf(x.get("time").get.toString), Integer.valueOf(x.get("value").get.toString))).toList))
          wbMediaStatistics.setPubOriginalArticleNumInPeroid(Integer.valueOf(row.getLong(8).toString))
          val doc = gaiaImpl.getWbDocument(wbMediaStatistics)
          document.put("wbStatisticsIn30Day", doc)
          document.put("latestArticle", JavaConversions.mapAsJavaMap(row.getList(9).get(0).asInstanceOf[Map[String, String]]))
          //数据从hive更新到mongodb的时间戳 13位
          document.put("updateTime", update_Time)
          list.add(document)
          if (list.size() == 3000) {
            wbMediaSave.insertWbAnalysisBatch(list)
            list.clear()
          }
        })
        if (list.size() != 0) {
          wbMediaSave.insertWbAnalysisBatch(list)
          list.clear()
        }
      })
    }
  }
}

object AnalysisSevenThirty {

  def apply() = new AnalysisSevenThirty

  def entry(args: Array[String], sc: SparkContext): Unit = {

    val hsc = new HiveContext(sc)

    /*
    sevenAnalysis: 是否执行7天统计计算
    thirtyAnalysis: 是否执行30天统计计算
    platType: 计算 微信：1 还是 微博：2
    mediaCategoryRegion:是否关联分类将每个分类随机5000个存储到mysql
    time: 计算时间  传入time 程序会自动从前2天开始算时间周期
     */
    val Array(sevenAnalysis, thirtyAnalysis, platType, mediaCategoryRegion, time) = args

    val update_Time: Long = Calendar.getInstance().getTimeInMillis()

    val Time_End: Integer = Integer.valueOf(new DataUtil().addTime(time, -2))
    val seven_Time_Start: Integer = Integer.valueOf(new DataUtil().addTime(String.valueOf(Time_End), -6))
    val thirty_Time_Start: Integer = Integer.valueOf(new DataUtil().addTime(String.valueOf(Time_End), -29))

    if (sevenAnalysis == "true") doSevenAnalysis(sc, hsc, Integer.valueOf(platType), seven_Time_Start, Time_End, update_Time)
    if (thirtyAnalysis == "true") doThirtyAnalysis(sc, hsc, Integer.valueOf(platType), thirty_Time_Start, Time_End, update_Time)
    if (mediaCategoryRegion == "true") doMediaCategoryRegion(sc, hsc, time)

  }

  def doSevenAnalysis(sc: SparkContext, hsc: HiveContext, platType: Integer, Time_Start: Integer, Time_End: Integer, update_Time: Long) = {

    val w = AnalysisSevenThirty()

    if (platType == Constant.wei_Xin_Type) {

      w.doWeiXinAnalysis(sc, hsc, Constant.seven_Save_Type, Time_Start, Time_End, update_Time)

    } else {

      w.doWeiBoAnalysis(sc, hsc, Constant.seven_Save_Type, Time_Start, Time_End, update_Time)

    }

  }

  def doThirtyAnalysis(sc: SparkContext, hsc: HiveContext, platType: Integer, Time_Start: Integer, Time_End: Integer, update_Time: Long) = {

    val w = AnalysisSevenThirty()

    if (platType == Constant.wei_Xin_Type) {

      w.doWeiXinAnalysis(sc, hsc, Constant.thirty_Save_Type, Time_Start, Time_End, update_Time)

    } else {

      w.doWeiBoAnalysis(sc, hsc, Constant.thirty_Save_Type, Time_Start, Time_End, update_Time)

    }

  }

  def doMediaCategoryRegion(sc: SparkContext, hsc: HiveContext, time: String) = {

    val config = new PropertiesConfiguration("ptb.properties")
    val address = config.getString("spring.datasource.url")
    val account = config.getString("spring.datasource.username")
    val password = config.getString("spring.datasource.password")
    val mysqlUrl = s"$address&user=$account&password=$password"
    val prop = new Properties()

    hsc.sql("use gaia")

    val basic_data = "SELECT pmid,categorys,type FROM gaia.mediapredicatecategory lateral view explode(category) id_table AS categorys where categorys !=\"\""
    hsc.sql(basic_data).repartition(10).registerTempTable("mediapredicatecategory_tmp")
    hsc.cacheTable("mediapredicatecategory_tmp")
    val time_End: Integer = Integer.valueOf(new DataUtil().addTime(time, -2))
    val time_Start: Integer = Integer.valueOf(new DataUtil().addTime(String.valueOf(time_End), -14))
    //按照分类和区间进行num排序 取前500
    //默认14天数据  需求是7天 就是为了让分类数据全面
    val sql = "select pmid,categorys as category,region,num as totalnum,type,current_timestamp() as add_time from (select pmid,categorys,num,region,type,row_number()over(partition by type,region order by num desc) as rank from (select pmid,categorys,num,region,type from (SELECT a.pmid,a.categorys,nvl(b.avgheadreadnuminperoid,0) AS num,CASE WHEN b.avgheadreadnuminperoid>=0 AND b.avgheadreadnuminperoid<50000 THEN 1 WHEN b.avgheadreadnuminperoid>=50000 AND b.avgheadreadnuminperoid<100000 THEN 2 WHEN b.avgheadreadnuminperoid>=100000 THEN 3 END AS region,1 as type FROM (SELECT pmid,categorys FROM mediapredicatecategory_tmp WHERE type=1) a LEFT JOIN (SELECT c.pmid,floor(sum(c.avgheadreadnuminperoid/7)) AS avgheadreadnuminperoid FROM gaia.wxanalysis_basic c WHERE c.time_date>=" + time_Start + " AND c.time_date<=" + time_End + " GROUP BY  pmid) b ON a.pmid=b.pmid WHERE b.pmid is NOT null union all SELECT d.pmid,d.categorys,nvl(e.num,0) AS num,CASE WHEN e.num>=0 AND e.num<1000 THEN 1 WHEN e.num>=1000 AND e.num<10000 THEN 2 WHEN e.num>=10000 AND e.num<50000 THEN 3 WHEN e.num>=50000 THEN 4 END AS region,2 as type FROM (SELECT pmid,categorys FROM mediapredicatecategory_tmp WHERE type=2) d LEFT JOIN (select pmid,(avgcommentnuminperoid+avgforwardnuminperoid+avglikenuminperoid) as num from (SELECT f.pmid,floor(sum(f.avgcommentnuminperoid/7)) AS avgcommentnuminperoid,floor(sum(f.avgforwardnuminperoid/7)) AS avgforwardnuminperoid,floor(sum(f.avglikenuminperoid/7)) AS avglikenuminperoid FROM gaia.wbanalysis_basic f WHERE f.time_date>=" + time_Start + " AND f.time_date<=" + time_End + " GROUP BY pmid) t ) e ON d.pmid=e.pmid WHERE e.pmid is NOT null) g ) k ) r where r.rank <= 500"
    MediaChannelDetailHandle.exeMysqlCommond(Array("truncate table media_category"))
    hsc.sql(sql).write.mode(SaveMode.Append).jdbc(mysqlUrl, "media_category", prop)
  }


  def main(args: Array[String]) {
    new SetUpUtil().runJob(entry, args, new SetUpUtil().getInitSC("AnalysisSevenThirty"))
  }

}