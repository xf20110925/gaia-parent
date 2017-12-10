package com.ptb.gaia.analysis.category

import java.util

import com.ptb.gaia.service.entity.media.RecommendCategoryMedia
import com.ptb.gaia.service.service.{GWbMediaService, GWxMediaService}
import com.ptb.gaia.service.{IGaia, IGaiaImpl}
import com.ptb.gaia.tokenizer.JSegTextAnalyser
import com.ptb.gaia.utils.SetUpUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.bson.Document

import scala.collection.convert.wrapAsScala._

case class token(kw: String)

object CategoryMedia {

  var knLibPath = "./";

  def main(args: Array[String]) {
    new SetUpUtil().runJob(entry, args, new SetUpUtil().getInitSC("categoryWx"))
  }


  def entry(args: Array[String], sc: SparkContext): Unit = {
    val hsc = new HiveContext(sc)
    val Array(doToken, doGenTokenDic, doTakeSample, doTrain, doPredicate, doPremiumCategory, modelPath, knowLibPath, doPredicateUpToMongoDb) = args;
    knLibPath = knowLibPath;
    if (doToken == "true") splitToken(sc, hsc)
    //"/Volumes/PAN/myNaiveBayesModel"
    if (doTakeSample == "true") doSample(sc, hsc, doGenTokenDic)
    if (doTrain == "true") train(sc, hsc, modelPath)
    if (doPredicate == "true") predicateCategory(sc, hsc, modelPath)
    if (doPremiumCategory == "true") premiunCategory(sc, hsc)
    if (doPredicateUpToMongoDb == "true") PredicateUpToMongoDb(sc, hsc)
  }

  def PredicateUpToMongoDb(sc: SparkContext, hsc: HiveContext) = {

    import hsc.implicits._
    val df = hsc.sql("select pmid,categorys,type from gaia.mediapredicatecategory LATERAL VIEW explode(category)idtable AS categorys where categorys!=\"\"")
    df.repartition(10).registerTempTable("category_temp")
    hsc.cacheTable("category_temp")

    hsc.sql("select pmid,cast(categorys as int) as categorys from category_temp where type=1")
      .mapPartitions(iter => {
        val wxMediaSave = new GWxMediaService();
        var list = new util.ArrayList[Document]()
        iter.foreach(row => {
          val document: Document = new Document
          document.put("pmid", row.getString(0))
          document.put("category", row.getInt(1))
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

        List.empty.iterator
      }).count()

    hsc.sql("select pmid,cast(categorys as int) as categorys from category_temp where type=2")
      .mapPartitions(iter => {
        val wbMediaSave = new GWbMediaService();
        var list = new util.ArrayList[Document]()
        iter.foreach(row => {
          val document: Document = new Document
          document.put("pmid", row.getString(0))
          document.put("category", row.getInt(1))
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

        List.empty.iterator
      }).count()

  }


  def premiunCategory(sc: SparkContext, hsc: HiveContext) = {

    //    import hsc.implicits._
    //    val sql = " select cate,rank,type,pmid,medianame from (\n select cate,pmid,medianame,2 as type, ROW_NUMBER() over (partition by cate order by  fansnum desc,zpznum desc) as rank from  \n ( select t1.medianame,t1.pmid as pmid ,t1.zpznum,t1.fansnum ,t2.cate as cate from \n       (select pmid,medianame,(avgcommentnuminperoid+avgforwardnuminperoid+avglikenuminperoid) as zpznum,fansnum as fansnum from gaia.wbmedia where fansnum > 10000 ) t1 \n   join (select pmid,cate FROM gaia.mediapredicatecategory LATERAL VIEW explode(category) atable AS cate where type = 2) t2 on t1.pmid = t2.pmid) t3\n union all \n select cate,pmid,medianame,1 as type, ROW_NUMBER() over (partition by cate order by  readnum desc,likenum desc) as rank from  \n ( select t1.medianame,t1.pmid as pmid ,t1.readnum as readnum ,t1.likenum as likenum ,t2.cate as cate from \n       (select pmid,medianame,avgheadreadnuminperoid as readnum ,avgheadlikenuminperoid as likenum from gaia.wxmedia) t1 \n   join (select pmid,cate FROM gaia.mediapredicatecategory LATERAL VIEW explode(category) atable AS cate where type = 1) t2 on t1.pmid = t2.pmid) t3\n ) p6 where rank < 5000";
    //    hsc.sql(sql).write.mode(SaveMode.Overwrite).saveAsTable("categoryRecommandResult")
    //    val igaia = new IGaiaImpl();
    //    igaia.removeAllRecommandCategory()
    //    hsc.sql("select cate,rank,type,pmid,medianame from categoryRecommandResult").mapPartitions(iter=>{
    //      val igaia = new IGaiaImpl();
    //      iter.foreach(row=>{
    //        val rcm  = new RecommendCategoryMedia()
    //        rcm.setCategory(row.getString(0))
    //        rcm.setRank(row.getInt(1))
    //        rcm.setType(row.getInt(2))
    //        rcm.setPmid(row.getString(3))
    //        rcm.setMedianame(row.getString(4))
    //        igaia.addBatchCategory(util.Arrays.asList(rcm))
    //      })
    //      List.empty.iterator
    //    }).count()
  }

  private def doSample(sc: SparkContext, hsc: HiveContext, doGenTokenDic: String): Unit = {
    import hsc.implicits._
    val tokensRDD = hsc.sql("select pmid,brief,medianame,stoken,btoken,type from gaia.t_m_token where type = 1").map(r => {
      val (pmid, brief, medianame, mtoken, btoken, sType) = (r.getAs[String](0), r.getAs[String](1), r.getAs[String](2), r.getAs[Seq[String]](3).toList, r.getAs[Seq[String]](4).toList, r.getInt(5))
      (pmid, brief, medianame, mtoken, btoken, sType)
    })
    //sc.broadcast(genTokenVetor(sc, hsc, tokensRDD)) :制作知识库，按照 stoken+btoken 做成一个表 按照分词wordcount 进行统计 ，按照分词排名
    //形成 index_kw 表
    /*
    map(一切,1)
    map(一点,2)
     */
    val bWordVector = if (doGenTokenDic == "true") sc.broadcast(genTokenVetor(sc, hsc, tokensRDD)) else sc.broadcast(getTokenVetor(sc, hsc));
    //按照 category.txt 文本 制作广播变量  变成 例如： 类型ID 类型名 的形式
    //形成表 t_category
    /*
      map(汽车, 0)
      map(轿车, 0)
      map(明星, 1)
      map(名人, 1)
      变成以上这种形式返回
      */
    val bCategoryDic = sc.broadcast(getCategoryDic(sc, hsc).map(kv => (kv._1, kv._2._1)));

    /*kv ：
  map(汽车, 0)
  map(轿车, 0)
  map(明星, 1)
  map(名人, 1)
  变成以上这种形式返回
  1、trueRdd : (0,[分词，1]) 意思就是  用 分类的词在分词中寻找  如果寻找到 就用  (0,[分词，1]) 表示 0可以看出 代表分类
  2、sampleRDD 每一行数据可能属于多个类型，数据模型例如：
  	22:162,1 179,1 218,1 42,1 30,1
  	19:162,1 3,1 152,1 44,1 220,1 78,1
  	冒号前的事分类ID  后面的分别是index 对应出现的次数
    总之 就是 分类：index和它出现的次数 如果分类词能在知识库找到 就是 index和1
    如果是13:  就表示 类型对应的类型词在分词库中找不到
 */

    val trueRdd = sc.parallelize(getCategoryDic(sc, hsc).flatMap(kv => {
      //map(1,一切)
      val wordVector = bWordVector.value
      //用分类的类别名称  在 知识库中寻找 是否能找到
      val hasToken = wordVector.get(kv._1);
      //如果能找到 就是 变为 (0,[分词，1])
      val ret = if (hasToken.nonEmpty) Option.apply((kv._2._1, Array((hasToken.get, 1)))) else Option.empty[(Int, Array[(Int, Int)])]
      ret
    }).toList);

    val sampleRDD = tokensRDD.flatMap {
      case (pmid, brief, medianame, mtoken, btoken, t) => {
        /*
          map(一切,1)
          map(一点,2)
           */
        val wordVector = bWordVector.value
        /*
          map(汽车, 0)
          map(轿车, 0)
          map(明星, 1)
          map(名人, 1)
          变成以上这种形式返回
          */
        val categoryDic = bCategoryDic.value
        val tokens = mtoken ++ btoken
        val tv = new util.HashMap[Int, Int]()
        //用媒体名和简介分词的分词去 分别 进行统计  如果在只是库中存在 就+1分
        //tv ： 1,2  2,1 3,1  都存储在Map
        //categroys ： （0） （1）
        //所有分词 在 分词知识库中寻找 然后 统计 分词 出现多少次  然后在分类中寻找
        val categroys = mtoken.flatMap(token => {
          wordVector.get(token).foreach(t => tv.put(t, tv.getOrDefault(t, 0) + 1))
          categoryDic.get(token)
        }).union(btoken.flatMap(token => {
          wordVector.get(token).foreach(t => tv.put(t, tv.getOrDefault(t, 0) + 1))
          categoryDic.get(token)
        })).distinct
        //
        if (categroys.size > 0) categroys.map(p => (p, tv.toArray)) else List.empty[(Int, Array[(Int, Int)])]
      }
    }.union(trueRdd)
    sampleRDD.map {
      case (cat, vcs) => {
        val build = new StringBuilder().append(cat + ":")
        vcs.foreach(vc => build.append("%d,%d\t".format(vc._1, vc._2)))
        build.toString()
      }
    }.toDF("sample").write.mode(SaveMode.Overwrite).saveAsTable("gaia.tmp_category_sample")
  }

  private def train(sc: SparkContext, hsc: HiveContext, modelPath: String): Unit = {
    val bWordVector = sc.broadcast(getTokenVetor(sc, hsc));
    val data = hsc.sql("select sample from gaia.tmp_category_sample").map(_.getString(0))
    val parsedData = data.flatMap { line =>
      val parts = line.split(':')
      val k: Array[Double] = new Array[Double](bWordVector.value.size + 1)
      if (parts.size <= 1) {
        Option.empty[LabeledPoint];
      } else {
        parts(1).split("\t").map(i => {
          val Array(index, point) = i.split(",");
          k(index.toInt) = point.toDouble
        })
        Option.apply(LabeledPoint(parts(0).toDouble, Vectors.dense(k)))
      }
    }

    // Split data into training (60%) and test (40%).
    /*
    val splits = parsedData.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0)
    val test = splits(1)*/
    val conf = new Configuration();
    val fs = FileSystem.get(conf)
    fs.delete(new Path(modelPath), true);
    val model = NaiveBayes.train(parsedData, lambda = 1.0, modelType = "multinomial")
    model.save(sc, modelPath)
  }

  private def getTokenVetor(sc: SparkContext, hsc: HiveContext): Map[String, Int] = {
    return hsc.sql("select index,kw from gaia.index_kw").map(k => (k.getAs[String](1), k.getAs[Int](0))).collect().toMap
  }

  private def genTokenVetor(sc: SparkContext, hsc: HiveContext, rdd1: RDD[(String, String, String, List[String], List[String], Int)]): Map[String, Int] = {
    import hsc.implicits._
    val dicRDD = rdd1.flatMap {
      case (pmid, brief, medianme, s1, s2, t) =>
        s1.++(s2)
    }.map(token(_)).toDF().registerTempTable("kword");
    hsc.sql("drop table gaia.index_kw")
    hsc.sql("create table gaia.index_kw as select kw as kw ,ROW_NUMBER() OVER(ORDER BY kw) as index,count as count from (select kw,count(*) as count from kword group by kw) t1 where count > 100 and count < 500000");
    return getTokenVetor(sc: SparkContext, hsc: HiveContext)
  }

  private def splitToken(sc: SparkContext, hsc: HiveContext): Unit = {
    //stoken 为媒体名字分词出来的结果  btoken为 媒体简介分出来的结果
    import hsc.implicits._
    val df = hsc.sql("select distinct pmid,brief,medianame,1 as type ,authinfo from gaia.wxMedia where time_date>=2016111000 union all select distinct pmid,brief,medianame,2 as type,authinfo from gaia.wbmedia where time_date>=2016111000 ").toDF("pmid", "brief", "medianame", "type", "authinfo");
    val TokensRDD = df.flatMap(row => {
      val (pmid, brief, medianame, t, authinfo) = (row.getAs[String](0), row.getAs[String](1), row.getAs[String](2), row.getInt(3), row.getString(4))
      val mediaToken = JSegTextAnalyser.i().getKeyWords(medianame).toList
      val briefToken = JSegTextAnalyser.i().getKeyWords(brief).toList;
      var auth = if (authinfo == null) "" else authinfo
      val authToken = JSegTextAnalyser.i().getKeyWords(auth).filter(t => {
        !t.equals("微博") && !t.equals("")
      }).toList
      if (t == 1)
        Option.apply((pmid, brief, medianame, mediaToken, briefToken, t));
      else if (t == 2 && !authToken.isEmpty) {
        Option.apply((pmid, brief, medianame, List.empty[String], authToken, t));
      } else {
        Option.empty[(String, String, String, List[String], List[String], Int)]
      }
    }).toDF("pmid", "brief", "medianame", "stoken", "btoken", "type").write.mode(SaveMode.Overwrite).saveAsTable("gaia.t_m_token")
  }

  private def getCategoryDic(sc: SparkContext, hsc: HiveContext): Map[String, (Int, String)] = {
    import hsc.implicits._
    val cateMap = sc.textFile(knLibPath).flatMap(line => {
      val elems = line.split("\\s+");
      val cateId = elems(0);
      val ts = elems.slice(1, elems.size)
      ts.map((cateId, _))
    }).toDF("categoryid", "category").write.mode(SaveMode.Overwrite).saveAsTable("gaia.t_category")



    hsc.sql("select category,(dense_rank() OVER(ORDER BY categoryid) -1) as id ,categoryid from gaia.t_category").write.mode(SaveMode.Overwrite).saveAsTable("gaia.t_category_map")
    /*
      map(汽车, 0 1001000000)
      map(轿车, 0 1001000000)
      map(明星, 1 1002000000)
      map(名人, 1 1002000000)
      变成以上这种形式返回
      */
    hsc.sql("select category,id,categoryid from gaia.t_category_map").map(r => (r.getString(0), (r.getInt(1), r.getString(2)))).collect().toMap
  }

  private def getCategoryMap(sc: SparkContext, hsc: HiveContext): Map[String, String] = {
    import hsc.implicits._
    val r = sc.textFile(knLibPath).flatMap(line => {
      val elems = line.split("\\s+");
      if (elems.size < 2) {
        Option.empty[(String, String)]
      } else {
        val cateId = elems(0);
        val category = elems(1)
        Option.apply((cateId, category))
      }
    }).collect().toMap
    r
  }

  private def predicateCategory(sc: SparkContext, hsc: HiveContext, modelPath: String): Unit = {
    import hsc.implicits._
    val tokensRDD = hsc.sql("select pmid,brief,medianame,stoken,btoken,type from gaia.t_m_token").map(r => {
      val (pmid, brief, median, mToken, bToken, t) = (r.getAs[String](0), r.getAs[String](1), r.getAs[String](2), r.getAs[Seq[String]](3).toList, r.getAs[Seq[String]](4).toList, r.getInt(5))
      (pmid, (brief, median, mToken, bToken, t))
    })

    val weibo = hsc.sql("select distinct pmid,itags from gaia.wbMedia where itags is not null and time_date>=2016111000 ").map(r => {
      val (pmid, itags) = (r.getAs[String](0), r.getAs[Seq[String]](1))
      (pmid, itags)
    });

    val willPredicateData = tokensRDD.leftOuterJoin(weibo).map {
      case (pmid, ((brief, median, mToken, bToken, t), optItags)) => {
        if (optItags.nonEmpty && optItags.get != null && optItags.get.nonEmpty) {
          (pmid, brief, median, mToken, bToken ++ optItags.get, t)
        } else {
          (pmid, brief, median, mToken, bToken, t)
        }
      }
    }

        /*
        map(一切,1)
        map(一点,2)
       */
    val bWordVector = sc.broadcast(getTokenVetor(sc, hsc));
       /*
        map(0 1001000000)
        map(0 1001000000)
        map(1 1002000000)
        map(1 1002000000)
        变成以上这种形式返回
       */
    val bReverseCategoryDic = sc.broadcast(getCategoryDic(sc, hsc).map(kv => (kv._2._1, kv._2._2)));
    /*  Map
        1001000000 汽车
        1001000000 轿车
        1002000000 明星
        1002000000 名人
     */
    val bCategoryMap = sc.broadcast(getCategoryMap(sc, hsc));
    /* Map
      map(汽车, （0，1001000000）)
      map(轿车, （0， 1001000000)
      map(明星, （1， 1002000000)
      map(名人, （1， 1002000000)
      变成以上这种形式返回
      */
    val bCategoryDic = sc.broadcast(getCategoryDic(sc, hsc));
    val sameModel = NaiveBayesModel.load(sc, modelPath)
    val result = willPredicateData.flatMap {
      case (pmid, brief, medianame, mtoken, btoken, t) => {
        val wordVector = bWordVector.value
        val reverseCategoryDic = bReverseCategoryDic.value
        val categoryMap = bCategoryMap.value;
        val categoryDic = bCategoryDic.value
        val tokens = mtoken ++ btoken
        val vcArray = new Array[Double](wordVector.size + 1)
        val preCategorys: util.List[(String, Double)] = new util.ArrayList[(String, Double)]
        tokens.foreach(token => {
          wordVector.get(token).foreach(index => {
            vcArray(index) = vcArray(index) + 1
          })
        })
        sameModel.predictProbabilities(Vectors.dense(vcArray)).foreachActive((index, rate) => {
          if (rate > 0.20) {
            preCategorys.add((reverseCategoryDic.getOrElse(index, ""), rate))
          }
        })

        val rates = preCategorys.toList.sortWith(_._2 > _._2).slice(0, 3).toArray

        val category = rates.map(kv => {
          kv._1
        }
        ).toList.toArray

        val categoryNames = rates.flatMap(kv => {
          val cateOpt = categoryMap.get(kv._1)
          if (cateOpt.isEmpty) {
            Option.empty[(String, Double)]
          } else {
            Option(cateOpt.get, kv._2)
          }
        }
        ).toList.toArray
        if (rates.size > 0) Option((pmid, medianame, brief, categoryNames, category, t)) else Option.empty[(String, String, String, Array[(String, Double)], Array[String], Int)]
      }
    }
    result.map(x =>
      (x._1, x._2, x._3, x._4, x._5.toStream.filter(x => (x != "" && x != null)).toList.toArray, x._6)
    ).filter(x => (x._5 != null && x._5.length != 0)).toDF("pmid", "medianame", "brief", "rates", "category", "type").write.mode(SaveMode.Overwrite).saveAsTable("gaia.mediaPredicateCategory")

  }
}
