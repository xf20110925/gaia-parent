package com.ptb.gaia.analysis.category


import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.ptb.gaia.common.Constant
import com.ptb.gaia.common.service.{IProcess, SaveWxToMongoImpl}
import com.ptb.gaia.tokenizer.JSegTextAnalyser
import com.ptb.gaia.utils.{DataUtil, SetUpUtil}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.{JavaConversions, mutable}
import scala.collection.convert.wrapAsScala._

/**
  * Created by MengChen on 2017/2/7.
  */
class PreCategoryLr extends java.io.Serializable {

  def CategoryContainFilter(tokens: List[String], map: mutable.Map[String, List[String]]): String = {

    for (elem <- tokens) {

      map.foreach(mapValue => {

        if (mapValue._2.contains(elem)) {
          return mapValue._1
        }

      })
    }

    "NULL"
  }


}


object PreCategoryLr {

  def apply() = new PreCategoryLr

  val saveWxToMongo: SaveWxToMongoImpl = new SaveWxToMongoImpl


  def entry(args: Array[String], sc: SparkContext): Unit = {

    val hsc = new HiveContext(sc)

    val path = args(0)

    val dirStrDf: RDD[String] = sc.textFile(path)

    var dirMap: scala.collection.mutable.Map[String, List[String]] = scala.collection.mutable.Map()


    dirStrDf.map(x => {

      val strArray = x.split(" ")
      (strArray(0), strArray(1).split(",").toList)

    }).collect().foreach(x => {
      dirMap += (x._1 -> x._2)
    })

    val broadcastMap: Broadcast[mutable.Map[String, List[String]]] = sc.broadcast(dirMap)

    val w = PreCategoryLr()

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val time_End = Integer.valueOf(dateFormat.format(new Date()).concat(Constant.uTime_suffix_23Time))
    val time_Start: Integer = Integer.valueOf(new DataUtil().addTime(String.valueOf(time_End).substring(0, 8), -30).concat(Constant.uTime_Suffix_0Time))
    val updateTimeStart = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA).parse(String.valueOf(time_Start).concat(Constant.pTime_Suffix_0Time)).getTime

    import hsc.implicits._

    //hsc.sql("select t.pmid,t.medianame from (select pmid,medianame,row_number()over(partition by pmid ORDER BY addtime desc) AS rank from gaia.wxmedia where updatetime >="+ updateTimeStart +" and time_date >="+ time_Start +") t where rank=1").map(row => {
    hsc.sql("select t.pmid,t.medianame from (select pmid,medianame,row_number()over(partition by pmid ORDER BY addtime desc) AS rank from gaia.wxmedia where tags is null and updatetime >=" + updateTimeStart + " and time_date >=" + time_Start + ") t where rank=1").map(row => {

      val (pmid, medianame) = (row.getAs[String](0), row.getAs[String](1))

      val tokens = JSegTextAnalyser.i().getKeyWords(medianame).toList

      if (tokens.isEmpty) {
        (pmid, medianame, "0")
      } else {
        val map = broadcastMap.value

        val categoryId = w.CategoryContainFilter(tokens.map(x => x.toUpperCase), map)

        if (!categoryId.equals("NULL")) {

          (pmid, medianame, categoryId)

        } else {

          (pmid, medianame, "0")
        }
      }

    }).toDF("pmid", "medianame", "categoryid").repartition(5).write.mode(SaveMode.Overwrite).saveAsTable("gaia.pre_category")

    val saveDf = hsc.sql("select t.pmid,collect_list(k.name) as id from gaia.pre_category t join gaia.secondclass_article k on t.categoryid = k.classid where t.categoryid !='0' group by t.pmid ")

    //saveDf.write.mode(SaveMode.Overwrite).saveAsTable("gaia.presaveDf")
    saveWxToMongo.saveWxDataToMongoLr(saveDf)

    hsc.clearCache()

    sc.stop()


  }


  def main(args: Array[String]) {

    new SetUpUtil().runJob(entry, args, new SetUpUtil().getInitSC("PreCategoryLr"))

  }


}