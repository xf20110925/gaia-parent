import java.util
import java.util.{Calendar, Properties}

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
import org.bson.Document

/**
  * Created by MengChen on 2016/10/14.
  * 7天和30天的计算 分别存储到Mongo
  */
class AnalysisSevenThirtyTest {

  def doWeiXinAnalysis(sc: SparkContext, hsc: HiveContext, saveType: Integer, Time_Start: Integer, Time_End: Integer, update_Time: Long): Unit = {

    hsc.sql("use gaia")
    val resultSql = "SELECT pmid,sum(a.pubnuminperoid) AS pubnuminperoid,floor(sum(a.avgheadreadnuminperoid/" + saveType + ")) AS avgheadreadnuminperoid,floor(sum(a.avgheadlikenuminperoid/" + saveType + ")) AS avgheadlikenuminperoid,collect_list(a.headreadpoints) AS headreadpoints,collect_list(a.secondreadpoints) AS secondreadpoints,collect_list(a.thirdreadpoints) AS thirdreadpoints,sum(a.puboriginalarticlenuminperoid) AS puboriginalarticlenuminperoid,sum(a.hotarticlenuminperoid) AS hotarticlenuminperoid,sum(a.pubarticlenuminperoid) AS pubarticlenuminperoid,sum(a.totallikenuminperoid) AS totallikenuminperoid,collect_list(a.latestarticle) AS latestarticle FROM (SELECT pmid,pubnuminperoid,avgheadreadnuminperoid,avgheadlikenuminperoid,headreadpoints,secondreadpoints,thirdreadpoints,puboriginalarticlenuminperoid,hotarticlenuminperoid,pubarticlenuminperoid,totallikenuminperoid,FIRST_VALUE(latestarticle)OVER(PARTITION BY pmid ORDER BY latestarticle['time'] DESC) AS latestarticle FROM gaia.wxanalysis_basic where time_date>=" + Time_Start + " and time_date<=" + Time_End + ") a GROUP BY pmid"

    if (saveType == Constant.seven_Save_Type) {
      hsc.sql(resultSql).repartition(10).mapPartitions(iter => {
        val wxMediaSave = new GWxMediaService()
        var list = new util.ArrayList[Document]()
        iter.foreach(row => {
          val document: Document = new Document
          document.put("pmid", row.getString(0))
          document.put("pubNumInPeroid", row.getLong(1))
          document.put("avgHeadReadNumInPeroid", row.getLong(2))
          document.put("avgHeadLikeNumInPeroid", row.getLong(3))
          val listHeadPoint = new util.ArrayList[util.Map[String, Long]]()
          var listitr = row.getList(4).iterator()
          while (listitr.hasNext) {
            val map = new util.HashMap[String, Long]()
            val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]]
            map.put("value", java.lang.Long.valueOf(Commentmap.getOrElse("value", 0).toString))
            map.put("time", java.lang.Long.valueOf(Commentmap.getOrElse("time", 0).toString))
            listHeadPoint.add(map)
          }
          document.put("headReadPoints", listHeadPoint)
          val listSecondReadPoints = new util.ArrayList[util.Map[String, Long]]()
          listitr = row.getList(5).iterator()
          while (listitr.hasNext) {
            val map = new util.HashMap[String, Long]()
            val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]]
            map.put("value", java.lang.Long.valueOf(Commentmap.getOrElse("value", 0).toString))
            map.put("time", java.lang.Long.valueOf(Commentmap.getOrElse("time", 0).toString))
            listSecondReadPoints.add(map)
          }
          document.put("secondReadPoints", listSecondReadPoints)
          val listThirdReadPoints = new util.ArrayList[util.Map[String, Long]]()
          listitr = row.getList(6).iterator()
          while (listitr.hasNext) {
            val map = new util.HashMap[String, Long]()
            val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]]
            map.put("value", java.lang.Long.valueOf(Commentmap.getOrElse("value", 0).toString))
            map.put("time", java.lang.Long.valueOf(Commentmap.getOrElse("time", 0).toString))
            listThirdReadPoints.add(map)
          }
          document.put("thirdReadPoints", listThirdReadPoints)
          document.put("pubOriginalArticleNumInPeroid", row.getLong(7))
          document.put("hotArticleNumInPeroid", row.getLong(8))
          document.put("pubArticleNumInPeroid", row.getLong(9))
          document.put("totalLikeNumInPeroid", row.getLong(10))
          val map = new util.HashMap[String, String]()
          val Commentmap = row.getList(11).get(0).asInstanceOf[scala.collection.immutable.Map[String, String]]
          map.put("value", Commentmap.getOrElse("value", 0).toString)
          map.put("time", Commentmap.getOrElse("time", 0).toString)
          document.put("latestArticle", map)
          //数据从hive更新到mongodb的时间戳 13位
          document.put("updateTime", update_Time)
          list.add(document)
          if (list.size() == 3000) {
            wxMediaSave.insertWxAnalysisBatch(list)
            list.clear()
            listHeadPoint.clear()
            listSecondReadPoints.clear()
            listThirdReadPoints.clear()
          }
        })
        if (list.size() != 0) {
          wxMediaSave.insertWxAnalysisBatch(list)
          list.clear()
        }
        List.empty.iterator
      }).count()

    } else if (saveType == Constant.thirty_Save_Type) {
      hsc.sql(resultSql).repartition(10).mapPartitions(iter => {
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
          val listHeadPoint = new util.ArrayList[Point]()
          var listitr = row.getList(4).iterator()
          while (listitr.hasNext) {
            val point = new Point()
            val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]]
            point.setTime(java.lang.Long.valueOf(Commentmap.getOrElse("time", 0).toString))
            point.setValue(java.lang.Long.valueOf(Commentmap.getOrElse("value", 0).toString))
            listHeadPoint.add(point)
          }
          wxMediaStatistics.setHeadReadPoints(listHeadPoint)
          val listSecondReadPoints = new util.ArrayList[Point]()
          listitr = row.getList(5).iterator()
          while (listitr.hasNext) {
            val point = new Point()
            val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]]
            point.setTime(java.lang.Long.valueOf(Commentmap.getOrElse("time", 0).toString))
            point.setValue(java.lang.Long.valueOf(Commentmap.getOrElse("value", 0).toString))
            listSecondReadPoints.add(point)
          }
          wxMediaStatistics.setSecondReadPoints(listSecondReadPoints)
          val listThirdReadPoints = new util.ArrayList[Point]()
          listitr = row.getList(6).iterator()
          while (listitr.hasNext) {
            val point = new Point()
            val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]]
            point.setTime(java.lang.Long.valueOf(Commentmap.getOrElse("time", 0).toString))
            point.setValue(java.lang.Long.valueOf(Commentmap.getOrElse("value", 0).toString))
            listThirdReadPoints.add(point)
          }
          wxMediaStatistics.setThirdReadPoints(listThirdReadPoints)
          wxMediaStatistics.setPubOriginalArticleNumInPeroid(Integer.valueOf(row.getLong(7).toString))
          wxMediaStatistics.setHotArticleNumInPeroid(Integer.valueOf(row.getLong(8).toString))
          wxMediaStatistics.setPubArticleNumInPeroid(Integer.valueOf(row.getLong(9).toString))
          wxMediaStatistics.setTotalLikeNumInPeroid(Integer.valueOf(row.getLong(10).toString))
          val doc = gaiaImpl.getWxDocument(wxMediaStatistics)
          document.put("wxStatisticsIn30Day", doc)
          val map = new util.HashMap[String, String]()
          val Commentmap = row.getList(11).get(0).asInstanceOf[scala.collection.immutable.Map[String, String]]
          map.put("value", Commentmap.getOrElse("value", 0).toString)
          map.put("time", Commentmap.getOrElse("time", 0).toString)
          document.put("latestArticle", map)
          //数据从hive更新到mongodb的时间戳 13位
          document.put("updateTime", update_Time)
          list.add(document)
          if (list.size() == 3000) {
            wxMediaSave.insertWxAnalysisBatch(list)
            list.clear()
            listHeadPoint.clear()
            listSecondReadPoints.clear()
            listThirdReadPoints.clear()
          }
        })

        if (list.size() != 0) {
          wxMediaSave.insertWxAnalysisBatch(list)
          list.clear()
        }

        List.empty.iterator
      }).count()
    }

  }

  def doWeiBoAnalysis(sc: SparkContext, hsc: HiveContext, saveType: Integer, Time_Start: Integer, Time_End: Integer, update_Time: Long): Unit = {

    hsc.sql("use gaia")
    val resultSql = "SELECT a.pmid,sum(a.pubnuminperoid) as pubnuminperoid,floor(sum(a.avgcommentnuminperoid/" + saveType + ")) as avgcommentnuminperoid,floor(sum(a.avgforwardnuminperoid/" + saveType + ")) as avgforwardnuminperoid,floor(sum(a.avglikenuminperoid/" + saveType + ")) as avglikenuminperoid,collect_list(a.totalcommentpoints) as totalcommentpoints,collect_list(a.totalforwardpoints) as totalforwardpoints,collect_list(a.totallikepoints) as totallikepoints,sum(a.puboriginalarticlenuminperoid) as puboriginalarticlenuminperoid,collect_list(a.latestarticle) AS latestarticle FROM (SELECT pmid,pubnuminperoid,avgcommentnuminperoid,avgforwardnuminperoid,avglikenuminperoid,totalcommentpoints,totalforwardpoints,totallikepoints,puboriginalarticlenuminperoid,FIRST_VALUE(latestarticle)OVER(PARTITION BY pmid ORDER BY latestarticle['time'] DESC) AS latestarticle FROM gaia.wbanalysis_basic where time_date>=" + Time_Start + " and time_date<=" + Time_End + ") a GROUP BY pmid"

    if (saveType == Constant.seven_Save_Type) {
      hsc.sql(resultSql).repartition(10).mapPartitions(iter => {
        val wbMediaSave = new GWbMediaService()
        var list = new util.ArrayList[Document]()
        iter.foreach(row => {
          val document: Document = new Document
          document.put("pmid", row.getString(0))
          document.put("pubNumInPeroid", row.getLong(1))
          document.put("avgCommentNumInPeroid", row.getLong(2))
          document.put("avgForwardNumInPeroid", row.getLong(3))
          document.put("avgLikeNumInPeroid", row.getLong(4))
          val listcommentnum = new util.ArrayList[util.Map[String, Long]]()
          var listitr = row.getList(5).iterator()
          while (listitr.hasNext) {
            val map = new util.HashMap[String, Long]()
            val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]]
            map.put("value", java.lang.Long.valueOf(Commentmap.getOrElse("value", 0).toString))
            map.put("time", java.lang.Long.valueOf(Commentmap.getOrElse("time", 0).toString))
            listcommentnum.add(map)
          }
          document.put("totalCommentPoints", listcommentnum)
          val listforwardnum = new util.ArrayList[util.Map[String, Long]]()
          listitr = row.getList(6).iterator()
          while (listitr.hasNext) {
            val map = new util.HashMap[String, Long]()
            val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]]
            map.put("value", java.lang.Long.valueOf(Commentmap.getOrElse("value", 0).toString))
            map.put("time", java.lang.Long.valueOf(Commentmap.getOrElse("time", 0).toString))
            listforwardnum.add(map)
          }
          document.put("totalForwardPoints", listforwardnum)
          val listlikenum = new util.ArrayList[util.Map[String, Long]]()
          listitr = row.getList(7).iterator()
          while (listitr.hasNext) {
            val map = new util.HashMap[String, Long]()
            val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]]
            map.put("value", java.lang.Long.valueOf(Commentmap.getOrElse("value", 0).toString))
            map.put("time", java.lang.Long.valueOf(Commentmap.getOrElse("time", 0).toString))
            listlikenum.add(map)
          }
          document.put("totalLikePoints", listlikenum)
          document.put("pubOriginalArticleNumInPeroid", row.getLong(8))
          val map = new util.HashMap[String, String]()
          val Commentmap = row.getList(9).get(0).asInstanceOf[scala.collection.immutable.Map[String, String]]
          map.put("value", Commentmap.getOrElse("value", 0).toString)
          map.put("time", Commentmap.getOrElse("time", 0).toString)
          document.put("latestArticle", map)
          //数据从hive更新到mongodb的时间戳 13位
          document.put("updateTime", update_Time)
          list.add(document)
          if (list.size() == 3000) {
            wbMediaSave.insertWbAnalysisBatch(list)
            list.clear()
            listcommentnum.clear()
            listforwardnum.clear()
            listlikenum.clear()
          }
        })
        if (list.size() != 0) {
          wbMediaSave.insertWbAnalysisBatch(list)
          list.clear()
        }
        List.empty.iterator
      }).count()

    } else if (saveType == Constant.thirty_Save_Type) {

      hsc.sql(resultSql).repartition(10).mapPartitions(iter => {
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

          val listcommentnum = new util.ArrayList[Point]()
          var listitr = row.getList(5).iterator()
          while (listitr.hasNext) {
            val point = new Point()
            val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]]
            point.setTime(java.lang.Long.valueOf(Commentmap.getOrElse("time", 0).toString))
            point.setValue(java.lang.Long.valueOf(Commentmap.getOrElse("value", 0).toString))
            listcommentnum.add(point)
          }
          wbMediaStatistics.setTotalCommentPoints(listcommentnum)

          val listforwardnum = new util.ArrayList[Point]()
          listitr = row.getList(6).iterator()
          while (listitr.hasNext) {
            val point = new Point()
            val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]]
            point.setTime(java.lang.Long.valueOf(Commentmap.getOrElse("time", 0).toString))
            point.setValue(java.lang.Long.valueOf(Commentmap.getOrElse("value", 0).toString))
            listforwardnum.add(point)
          }
          wbMediaStatistics.setTotalForwardPoints(listforwardnum)

          val listlikenum = new util.ArrayList[Point]()
          listitr = row.getList(7).iterator()
          while (listitr.hasNext) {
            val point = new Point()
            val Commentmap = listitr.next().asInstanceOf[scala.collection.immutable.Map[String, String]]
            point.setTime(java.lang.Long.valueOf(Commentmap.getOrElse("time", 0).toString))
            point.setValue(java.lang.Long.valueOf(Commentmap.getOrElse("value", 0).toString))
            listlikenum.add(point)
          }
          wbMediaStatistics.setTotalLikePoints(listlikenum)
          wbMediaStatistics.setPubOriginalArticleNumInPeroid(Integer.valueOf(row.getLong(8).toString))

          val doc = gaiaImpl.getWbDocument(wbMediaStatistics)
          document.put("wbStatisticsIn30Day", doc)

          val map = new util.HashMap[String, String]()
          val Commentmap = row.getList(9).get(0).asInstanceOf[scala.collection.immutable.Map[String, String]]
          map.put("value", Commentmap.getOrElse("value", 0).toString)
          map.put("time", Commentmap.getOrElse("time", 0).toString)
          document.put("latestArticle", map)
          //数据从hive更新到mongodb的时间戳 13位
          document.put("updateTime", update_Time)
          list.add(document)
          if (list.size() == 3000) {
            wbMediaSave.insertWbAnalysisBatch(list)
            list.clear()
            listcommentnum.clear()
            listforwardnum.clear()
            listlikenum.clear()
          }
        })
        if (list.size() != 0) {
          wbMediaSave.insertWbAnalysisBatch(list)
          list.clear()
        }
        List.empty.iterator
      }).count()
    }
  }
}

object AnalysisSevenThirtyTest {

  def apply() = new AnalysisSevenThirtyTest

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
    val seven_Time_Start: Integer = Integer.valueOf(new DataUtil().addTime(String.valueOf(Time_End), -7))
    val thirty_Time_Start: Integer = Integer.valueOf(new DataUtil().addTime(String.valueOf(Time_End), -30))

    if (sevenAnalysis == "true") doSevenAnalysis(sc, hsc, Integer.valueOf(platType), seven_Time_Start, Time_End, update_Time)
    if (thirtyAnalysis == "true") doThirtyAnalysis(sc, hsc, Integer.valueOf(platType), thirty_Time_Start, Time_End, update_Time)
    if (mediaCategoryRegion == "true") doMediaCategoryRegion(sc, hsc, time)

  }

  def doSevenAnalysis(sc: SparkContext, hsc: HiveContext, platType: Integer, Time_Start: Integer, Time_End: Integer, update_Time: Long) = {

    val w = AnalysisSevenThirtyTest()

    if (platType == Constant.wei_Xin_Type) {

      w.doWeiXinAnalysis(sc, hsc, Constant.seven_Save_Type, Time_Start, Time_End, update_Time)

    } else {

      w.doWeiBoAnalysis(sc, hsc, Constant.seven_Save_Type, Time_Start, Time_End, update_Time)

    }

  }

  def doThirtyAnalysis(sc: SparkContext, hsc: HiveContext, platType: Integer, Time_Start: Integer, Time_End: Integer, update_Time: Long) = {

    val w = AnalysisSevenThirtyTest()

    if (platType == Constant.wei_Xin_Type) {

      w.doWeiXinAnalysis(sc, hsc, Constant.thirty_Save_Type, Time_Start, Time_End, update_Time)

    } else {

      w.doWeiBoAnalysis(sc, hsc, Constant.thirty_Save_Type, Time_Start, Time_End, update_Time)

    }

  }

  def doMediaCategoryRegion(sc: SparkContext, hsc: HiveContext, time: String) = {

    val config = new PropertiesConfiguration("ptb.properties");
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
    hsc.sql(sql).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl, "media_category", prop)
  }


  def main(args: Array[String]) {
    new SetUpUtil().runJob(entry, args, new SetUpUtil().getInitSC("AnalysisSevenThirty"))
  }

}