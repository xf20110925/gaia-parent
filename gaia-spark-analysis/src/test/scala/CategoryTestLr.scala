import com.ptb.gaia.common.Constant
import com.ptb.gaia.tokenizer.JSegTextAnalyser
import com.ptb.gaia.utils.{LRClassifyUtils, SetUpUtil, VectorizerUtils}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

import scala.collection.convert.wrapAsScala._


/**
  * Created by MengChen on 2017/2/13.
  */
class CategoryTestLr {

}


object CategoryTestLr {

  var vocabSize: Long = 0L

  var classNum: Long = 0L

  def entry(args: Array[String], sc: SparkContext): Unit = {

    val Array(lrModelPath, vecModelPath, idfModelPath) = args

    val hsc = new HiveContext(sc)

    PredicateTest(sc, hsc, lrModelPath, vecModelPath, idfModelPath, 30010L, 6L)

    hsc.clearCache()
    sc.stop()
  }


  private def PredicateTest(sc: SparkContext, hsc: HiveContext, modelPath: String, vecModelPath: String, idfModelPath: String, vocabSize: Long, classNum: Long): Unit = {

    println("Predicate Vocabulary Num: " + vocabSize + "  Predicate Class: " + classNum)

    val predicate_Rdd = hsc.sql("select classid as pmid,content as articleurl,content from gaia.test_hive ").map(row => {
      val (pmid, articleurl, content) = (row.getAs[String](0), row.getAs[String](1), row.getAs[String](2))
      val tokens = JSegTextAnalyser.i().getKeyWords(content).toList
      (pmid, articleurl, tokens)
    })

    val vectorizer = new VectorizerUtils().setMinDocFreq(Constant.minDocFreq).setToTFIDF(true).setVocabSize(vocabSize.toInt)

    val (cvModel, idfModel, lrModel) = vectorizer.load(vecModelPath, idfModelPath, modelPath, sc)

    val tokensLP_Rdd = vectorizer.predicateVectorize(predicate_Rdd, cvModel, idfModel)

    val lrUtils = new LRClassifyUtils(classNum.toInt)

    predictTest(tokensLP_Rdd, lrModel, hsc)

  }


  /**
    * 单文本预测，输入一段文本预测其类别
    *
    * @param tokensLP LabeledPoint数据
    * @param model    LRModel
    * @return 预测类别ID
    */
  def predictTest(tokensLP: RDD[(String, String, LabeledPoint)], model: LogisticRegressionModel, hsc: HiveContext): Unit = {

    import hsc.implicits._
    tokensLP.map {
      case (pmid, articleurl, LabeledPoint(label, feature)) =>
        val prediction = model.predict(feature)
        (pmid, articleurl, prediction)
    }.toDF("pmid", "articleurl", "prediction").write.mode(SaveMode.Overwrite).saveAsTable("gaia.LRresultTest")

  }





  def main(args: Array[String]) {

    new SetUpUtil().runJob(entry, args, new SetUpUtil().getInitSC("CategoryLr"))

  }


}