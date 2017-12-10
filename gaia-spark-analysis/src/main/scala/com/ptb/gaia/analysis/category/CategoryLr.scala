package com.ptb.gaia.analysis.category

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import com.ptb.gaia.analysis.calculation.MediaChannelDetailHandle.mysqlUrl
import com.ptb.gaia.common.service.SaveWxToMongoImpl
import com.ptb.gaia.common.Constant
import com.ptb.gaia.partition.{RandomSegmentPartitioner, WordSegmentPartitioner}
import com.ptb.gaia.search.ISearchImpl
import com.ptb.gaia.service.entity.article.GWxArticle
import com.ptb.gaia.tokenizer.JSegTextAnalyser
import com.ptb.gaia.utils.{DataUtil, LRClassifyUtils, SetUpUtil, VectorizerUtils}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

import scala.collection.convert.wrapAsScala._


object CategoryLr {

  var vocabSize: Long = 0L

  var classNum: Long = 0L

  def entry(args: Array[String], sc: SparkContext): Unit = {

    val Array(doBasicData, toTrain, doPredicate, isSaveMongo, endTime, interval, lrModelPath, vecModelPath, idfModelPath, numIteration, regParam) = args

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

    var time_End: Integer = 0

    if (!endTime.toString.equals("null")) {
      time_End = Integer.valueOf(endTime.concat(Constant.uTime_suffix_23Time))
    } else {
      time_End = Integer.valueOf(dateFormat.format(new Date()).concat(Constant.uTime_suffix_23Time))
    }

    val time_Start: Integer = Integer.valueOf(new DataUtil().addTime(String.valueOf(time_End).substring(0, 8), -interval.toInt).concat(Constant.uTime_Suffix_0Time))

    val hsc = new HiveContext(sc)

    if (doBasicData == "true") BasicData(sc, hsc)
    if (toTrain == "true") train(sc, hsc, lrModelPath, vecModelPath, idfModelPath, numIteration.toInt, regParam.toDouble)
    if (doPredicate == "true") Predicate(time_Start, time_End, sc, hsc, lrModelPath, vecModelPath, idfModelPath)
    if (isSaveMongo == "true") saveToMongo(hsc)
    hsc.clearCache()
    sc.stop()
  }

  private def BasicData(sc: SparkContext, hsc: HiveContext): Unit = {
    import hsc.implicits._

    val rdd = hsc.sql("select cast(k.id as bigint) as classid,t.content from gaia.class_article_train2 t join gaia.secondclass_article k on t.classid=k.classid where t.content is not null")
      .map(row => (row.getAs[Long](0), row))
      .partitionBy(new RandomSegmentPartitioner(15))
      .map(row => {
        val (classid, content) = (row._2.getAs[Long](0), row._2.getAs[String](1))
        val tokens = JSegTextAnalyser.i().getKeyWords(content).toList
        (classid, tokens.filter(x => !isNumeric(x)), tokens.size)
      }).toDF("classid", "tokens", "size").repartition(5).write.mode(SaveMode.Overwrite).saveAsTable("gaia.category_basic")

  }

  private def train(sc: SparkContext, hsc: HiveContext, modelPath: String, vecModelPath: String, idfModelPath: String, numIteration: Integer, regParam: Double): Unit = {


    vocabSize = hsc.sql("select sum(size) as count from gaia.category_basic ").first().getAs[Long](0)

    classNum = hsc.sql("select count(distinct classid) as class_count from gaia.category_basic ").first().getAs[Long](0)

    println("Train Vocabulary Num: " + vocabSize + "  Train Class: " + classNum + " NumIteration : " + numIteration + " RegParam: " + regParam)

    val train_Rdd = hsc.sql("select classid,tokens from gaia.category_basic t").map(row => {
      val (classid, tokens) = (row.getAs[Long](0), row.getSeq[String](1))
      (classid, tokens.toList)
    })
    val vectorizer = new VectorizerUtils().setMinDocFreq(Constant.minDocFreq).setToTFIDF(true).setVocabSize(vocabSize.toInt)
    val (vectorizedRDD, idfModel, cvModel) = vectorizer.trainVectorize(train_Rdd)
    val lrUtils = new LRClassifyUtils(classNum.toInt)
    val model: LogisticRegressionModel = lrUtils.train(vectorizedRDD, numIteration, regParam)

    vectorizer.save(vecModelPath, idfModelPath, cvModel, idfModel, sc)
    lrUtils.save(sc, model, modelPath)
  }

  private def Predicate(time_Start: Integer, time_End: Integer, sc: SparkContext, hsc: HiveContext, modelPath: String, vecModelPath: String, idfModelPath: String): Unit = {

    vocabSize = hsc.sql("select sum(size) as count from gaia.category_basic ").first().getAs[Long](0)

    classNum = hsc.sql("select count(distinct classid) as class_count from gaia.category_basic ").first().getAs[Long](0)

    println("Predicate Vocabulary Num: " + vocabSize + "  Predicate Class: " + classNum + " time_Start: " + time_Start + " time_End: " + time_End)

    import hsc.implicits._

    val rdd = hsc.sql("select t.pmid,t.articleurl from gaia.wxArticle t join gaia.pre_category k on t.pmid = k.pmid where k.categoryid='0' and t.time_date>=" + time_Start + " and t.time_date<=" + time_End + " and t.content is not null ").map(row => {
      (row.getAs[String](0), row.getAs[String](1))
    }).reduceByKey((str1, str2) => {
      String.format("%s:::%s", str1, str2)
    }).mapPartitions(iter => {
      val searchImpl = new ISearchImpl()
      iter.flatMap(x => {
        var urls = new util.HashSet[java.lang.String]()
        x._2.split(":::").toList.foreach(url => {
          urls.add(url)
        })
        val list: List[String] = searchImpl.searchArticleContentByUrl(urls, classOf[GWxArticle]).toList

        if (list.isEmpty) {
          List.empty[(String, String, String)]
        }
        list.map(content => {
          val str = content.split(":::")
          (x._1, str(0), str(1))
        })
      })
    }).toDF("pmid", "articleurl", "content").write.mode(SaveMode.Overwrite).saveAsTable("gaia.mid_category")

    val predicate_Rdd = hsc.sql("select t.pmid,t.articleurl,t.content from gaia.mid_category t ").map(row => {
      val (pmid, articleurl, content) = (row.getAs[String](0), row.getAs[String](1), row.getAs[String](2))
      val tokens = JSegTextAnalyser.i().getKeyWords(content).toList
      (pmid, articleurl, tokens)
    })

    val vectorizer = new VectorizerUtils().setMinDocFreq(Constant.minDocFreq).setToTFIDF(true).setVocabSize(vocabSize.toInt)

    val (cvModel, idfModel, lrModel) = vectorizer.load(vecModelPath, idfModelPath, modelPath, sc)

    val tokensLP_Rdd = vectorizer.predicateVectorize(predicate_Rdd, cvModel, idfModel)

    val lrUtils = new LRClassifyUtils(classNum.toInt)

    lrUtils.predict(tokensLP_Rdd, lrModel, hsc)

  }


  private def saveToMongo(hsc: HiveContext): Unit = {

    /*
      三种类型的数据：
      1、rank1 小于50篇文章的 获取第一分类
      2、剩下的媒体 rank1-rank2 大于等于10的 说明有差距 获取第一分类
      3、剩下的媒体 rank1-rank2 小于10的 说明分类比较接近 获取第一、第二分类
      所有的数据 必须是 Pre预测中没有预测到的媒体 所以 如果在 Pre中存在的媒体就用 Pre预测的分类
     */

    val saveWxToMongo: SaveWxToMongoImpl = new SaveWxToMongoImpl

    //预判断中 判断分类不等于0 的pmid  也就是在与前期预测判断出分类的 pmid
    val preCategory = hsc.sql("select t.pmid,collect_list(k.id) as id from gaia.pre_category t join gaia.secondclass_article k on t.categoryid = k.classid where t.categoryid !='0' group by t.pmid")
    preCategory.registerTempTable("preCategory")
    hsc.cacheTable("preCategory")

    //保留pmid 分类 对象文章数排名前2的数据
    val savePmidTwoRank = hsc.sql("SELECT pmid,prediction,cnt,rank FROM (SELECT pmid,prediction,cnt,row_number()over(partition by pmid ORDER BY cnt desc) AS rank FROM (SELECT pmid,prediction,count(1) AS cnt FROM gaia.LRresult t GROUP BY pmid,prediction) t1) t2 WHERE t2.rank<=2")
    savePmidTwoRank.registerTempTable("savePmidTwoRank")
    hsc.cacheTable("savePmidTwoRank")

    //  从 savePmidTwoRank 获取 pmid排名第一 文章数小于50篇的 pmid
    val articleNumlt20Df = hsc.sql("SELECT DISTINCT t2.pmid FROM savePmidTwoRank t2 WHERE t2.rank=1 AND t2.cnt<=50")
    articleNumlt20Df.registerTempTable("articleNumlt20Df")

    // 第一种分类情况: 小于50篇文章的 pmid  我们只需要排名第一的分类就可以 但是 如果前期的预分类已经得出结果  就用预分类的结果 剩下的pmid 采用模型分类结果
    val articleNum20Result = hsc.sql("select t2.pmid,t2.id from (SELECT t1.pmid,collect_list(t1.prediction) AS id FROM articleNumlt20Df t JOIN savePmidTwoRank t1 ON t.pmid=t1.pmid WHERE t1.rank=1 GROUP BY t1.pmid) t2 left join preCategory t3 on t2.pmid=t3.pmid WHERE t3.pmid is null")
    articleNum20Result.registerTempTable("articleNum20Result")


    // 除了 文章数在50篇一下的pmid 其他媒体的筛选 col1:rank=1 col2:rank=2 col3:rank1-rank2 articleNum
    val articleNumOtherPmid = hsc.sql("select t4.pmid,col1,col2,(col1-col2) as col3 from (select pmid,max(if(t3.rank=1,t3.cnt,0)) as col1,max(if(t3.rank=2,t3.cnt,0)) as col2 FROM (SELECT t1.pmid,t1.prediction,t1.cnt,t1.rank FROM savePmidTwoRank t1 left join articleNumlt20Df t2 on t1.pmid=t2.pmid WHERE t2.pmid is null) t3 GROUP BY pmid) t4 ")
    articleNumOtherPmid.registerTempTable("articleNumOtherPmid")

    // 第二种分类情况: col3 rank1-rank2>=10 说明 第一分类和第二分类的相差比较大
    val diffGte10Result = hsc.sql("select t5.pmid,t5.id from (select t3.pmid,collect_list(prediction) as id from (select t2.pmid,t2.prediction from (select pmid from articleNumOtherPmid t where t.col3>=10) t1 join savePmidTwoRank t2 on t1.pmid=t2.pmid where t2.rank=1) t3 group by t3.pmid) t5 left join preCategory t6 on t5.pmid=t6.pmid WHERE t6.pmid is null ")
    diffGte10Result.registerTempTable("diffGte10Result")


    // 第三种分类情况: col3 rank1-rank2<10 说明 第一分类和第二分类的相差比较小
    val difflt10Result = hsc.sql("SELECT t5.pmid,t5.id FROM (SELECT t3.pmid,collect_list(prediction) AS id FROM (SELECT t2.pmid,t2.prediction FROM (SELECT pmid FROM articleNumOtherPmid t WHERE t.col3<10) t1 JOIN savePmidTwoRank t2 ON t1.pmid=t2.pmid WHERE t2.rank<=2) t3 GROUP BY  t3.pmid) t5 left join preCategory t6 on t5.pmid=t6.pmid WHERE t6.pmid is null ")
    difflt10Result.registerTempTable("difflt10Result")

    hsc.read.jdbc(mysqlUrl, "category_black", new Properties).select("pmid").distinct().registerTempTable("category_black")

    // 最后汇总前三种情况的数据
    val saveDf = hsc.sql("select t8.pmid,t8.id from (select t6.pmid,collect_list(t7.name) as id from (SELECT t4.pmid,id5 FROM (SELECT t.pmid,t.id FROM articleNum20Result t UNION all SELECT t1.pmid,t1.id FROM diffGte10Result t1 UNION all SELECT t2.pmid,t2.id FROM difflt10Result t2) t4 LATERAL VIEW explode(id)idtable AS id5 ) t6 join gaia.secondclass_article t7 on t6.id5=t7.id group by pmid) t8 left join category_black t9 on t8.pmid=t9.pmid where t9.pmid is null ")

    //存留一个临时结果表
    saveDf.write.mode(SaveMode.Overwrite).saveAsTable("gaia.saveDf")

    saveWxToMongo.saveWxDataToMongoLr(saveDf)

  }


  private def isNumeric(str: String): Boolean = {
    if (str.contains("%")) {
      return true
    }
    str.foreach(chr => {
      if (chr < 48 || chr > 57) {
        if (chr != 46)
          return false
      }
    })
    true
  }


  def main(args: Array[String]) {

    new SetUpUtil().runJob(entry, args, new SetUpUtil().getInitSC("CategoryLr"))

  }

}
