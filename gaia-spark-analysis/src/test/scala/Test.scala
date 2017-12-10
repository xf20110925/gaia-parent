import com.ptb.gaia.common.service.SaveWxToMongoImpl
import com.ptb.gaia.search.ISearchImpl
import com.ptb.gaia.utils.SetUpUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by MengChen on 2016/10/13.
  */
class Test extends Serializable{

  def getInstanct(): ISearchImpl ={

    new ISearchImpl()
  }


}


object Test extends Serializable{

  def apply() = new Test

  def entry(args: Array[String], sc: SparkContext): Unit = {

//    val time = new SimpleDateFormat("yyyyMMddHH",Locale.CHINA).format(new Date())
//    val dataFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA)
//    val time_start = dataFormat.parse(time.concat("0000")).getTime
//    val time_end = dataFormat.parse(time.concat("5959")).getTime
//
//
//
//
//    println("!!!!!!!!!!!!!!!!!!!!!!!!!!")
//    val Array(doBasicData, toTrain, doPredicate, isSaveMongo, endTime, interval, lrModelPath, vecModelPath, idfModelPath, numIteration, regParam) = args
//    println("#######################"+doBasicData+" "+toTrain+" "+endTime+" "+vecModelPath)
//
//    println("%%%%%%%%%%%%%%%%%%%"+sc.getConf.get("spark.executor.extraClassPath"))

    val hsc = new HiveContext(sc)

//    val rdd = hsc.createDataFrame(Seq(
//      ("MTA1NTIyMTg2MQ==", "http://mp.weixin.qq.com/s?__biz=MTA1NTIyMTg2MQ==&mid=2652089216&idx=5&sn=7f3c60346079075a75a49847fe0b854e#rd"),
//      ("MTA1NTIyMTg2MQ==", "http://mp.weixin.qq.com/s?__biz=MTA1NTIyMTg2MQ==&mid=2652089264&idx=7&sn=7c25602181bd5f57b98e7ee3e477a489#rd"),
//      ("MTA1NTIyMTg2MQ==", "http://mp.weixin.qq.com/s?__biz=MTA1NTIyMTg2MQ==&mid=2652089264&idx=8&sn=db07e1349009d53c5dde94aa6014420b#rd"),
//      ("MTA3NDM1MzUwMQ==", "http://mp.weixin.qq.com/s?__biz=MTA3NDM1MzUwMQ==&mid=2651934512&idx=1&sn=63044df87a012196842f8852ef0acb2d#rd"),
//      ("MTA3NDM1MzUwMQ==", "http://mp.weixin.qq.com/s?__biz=MTA3NDM1MzUwMQ==&mid=2651934524&idx=2&sn=756a1610861acf7b8533a21db473d1a1#rd")
//    )).map(row => {
//      (row.getAs[String](0), row.getAs[String](1))
//    }).reduceByKey((str1, str2) => {
//      String.format("%s:::%s", str1, str2)
//    }).mapPartitions(iter=>{
//      val searchImpl = new ISearchImpl()
//      iter.flatMap(x=>{
//        var urls = new util.HashSet[java.lang.String]()
//        x._2.split(":::").toList.foreach(url => {
//          urls.add(url)
//        })
//        val list: List[String] = searchImpl.searchArticleContentByUrl(urls, classOf[GWxArticle]).toList
//        if (list.isEmpty) {
//          List.empty[(String, String, String)]
//        }
//        list.map(content => {
//          val str = content.split(":::")
//          (x._1, str(0), str(1))
//        })
//      })
//    }).toDF("pmid", "articleurl", "content").write.mode(SaveMode.Overwrite).saveAsTable("gaia.mid_category")
    val saveWxToMongo: SaveWxToMongoImpl = new SaveWxToMongoImpl
    val saveDf = hsc.sql("select t.pmid,collect_list(t1.name) as id from gaia.test_hive1 t join gaia.class_article t1 on t.classid=t1.classid group by pmid")

    saveWxToMongo.saveWxDataToMongoLr(saveDf)

    hsc.clearCache()
    sc.stop()

  }


  def main(args: Array[String]) {

    new SetUpUtil().runJob(entry, args, new SetUpUtil().getInitSC("Test"))

  }


}
