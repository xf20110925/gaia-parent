package com.ptb.gaia.utils

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF, IDFModel}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.reflect.io.File

/**
  * Created by MengChen on 2016/7/25.
  */
class VectorizerUtils(
                       private var minDocFreq: Int,
                       private var vocabSize: Int,
                       private var toTFIDF: Boolean
                     ) extends Serializable {

  def this() = this(minDocFreq = 2, vocabSize = 5000, toTFIDF = true)

  def setMinDocFreq(minDocFreq: Int): this.type = {
    require(minDocFreq > 0, "最小文档频率必须大于0")
    this.minDocFreq = minDocFreq
    this
  }

  def setVocabSize(vocabSize: Int): this.type = {
    require(vocabSize >= 30000, "词汇表大小不小于30000")
    this.vocabSize = vocabSize
    this
  }

  def setToTFIDF(toTFIDF: Boolean): this.type = {
    this.toTFIDF = toTFIDF
    this
  }

  def getMinDocFreq: Int = this.minDocFreq

  def getVocabSize: Int = this.vocabSize

  def getToTFIDF: Boolean = this.toTFIDF


  /**
    * 生成向量模型
    *
    * @param df        DataFrame
    * @param vocabSize 词汇表大小
    * @return 向量模型
    */
  def genCvModel(df: DataFrame, vocabSize: Int): CountVectorizerModel = {
    val cvModel = new CountVectorizer()
      .setInputCol("tokens")
      .setOutputCol("rawFeatures")
      .setVocabSize(vocabSize)
      .fit(df)

    cvModel
  }


  /*
      预测专用 MengChen 2017-1-3 ^_^
   */
  def predicateGenCvModel(df: DataFrame, vocabSize: Int): CountVectorizerModel = {
    val cvModel = new CountVectorizer()
      .setInputCol("tokens")
      .setOutputCol("features")
      .setVocabSize(vocabSize)
      .fit(df)

    cvModel
  }


  /**
    * 将词频数据转化为特征LabeledPoint
    *
    * @param df      DataFrame数据
    * @param cvModel 向量模型
    * @return LabeledPoint
    */
  def toTFDF(df: DataFrame, cvModel: CountVectorizerModel): DataFrame = {
    val documents = cvModel.transform(df).select("id", "rawFeatures")
    documents
  }

  def predicateToTFDF(df: DataFrame, cvModel: CountVectorizerModel): DataFrame = {
    val documents = cvModel.transform(df).select("pmid","articleurl", "rawFeatures")
    documents
  }

  /**
    * 根据特征向量生成tf-idf模型
    *
    * @param df 文档LabeledPoint
    * @return IDFModel
    */
  def genIDFModel(df: DataFrame): IDFModel = {

    val IDF = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    val idfModel = IDF.fit(df)

    idfModel
  }


  /**
    * 将词频LabeledPoint转化为TF-IDF的LabeledPoint
    *
    * @param df       词频LabeledPoint
    * @param idfModel TF-IDF模型
    * @return TF-IDF的LabeledPoint
    *         结果为 分类,TF-IDF(词汇范围,词向量,逆文档词频)
    */
  def toTFIDFLP(df: DataFrame, idfModel: IDFModel): RDD[LabeledPoint] = {

    val tfidf = idfModel.transform(df)

    val tfidfDocs = tfidf.select("id", "features").map { row =>

      val (id, features) = (row.getAs[Long](0),row.getAs[SparseVector](1))

      LabeledPoint(id,features)
    }

    tfidfDocs
  }


  def predicateToTFIDFLP(df: DataFrame, idfModel: IDFModel) : RDD[(String,String,LabeledPoint)]= {

    val tfidf = idfModel.transform(df)

    val tfidfDocs = tfidf.select("pmid","articleurl", "features").map { row =>
      val (pmid, articleurl ,features) = (row.getAs[String](0),row.getAs[String](1),row.getAs[SparseVector](2))

      (pmid,articleurl,LabeledPoint(0,features))
    }

    tfidfDocs
  }


  /**
    * 已分词中文数据向量化
    *
    * @param data 已分词数据
    * @return
    */
  def trainVectorize(data: RDD[(Long, List[String])]): (RDD[LabeledPoint], IDFModel, CountVectorizerModel) = {

    val sc = data.context
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val tokenDF = data.toDF("id", "tokens")
    var startTime = System.nanoTime()
    val cvModel = genCvModel(tokenDF, vocabSize)
    val cvTime = (System.nanoTime() - startTime) / 1e9
    startTime = System.nanoTime()
    println(s"生成cvModel完成！\n\t 耗时: $cvTime sec\n")

   var tokensDF = toTFDF(tokenDF, cvModel)
    val lpTime = (System.nanoTime() - startTime) / 1e9
    startTime = System.nanoTime()
    println(s"转化tokensDF完成！\n\t 耗时: $lpTime sec\n")

    //转换为TFIDF
    var idfModel: IDFModel = null
    var tokensLP: RDD[LabeledPoint] = null
    if (toTFIDF) {
      idfModel = genIDFModel(tokensDF)
      tokensLP = toTFIDFLP(tokensDF, idfModel)
    }
    val idfTime = (System.nanoTime() - startTime) / 1e9
    println(s"转化TFIDF完成！\n\t 耗时: $idfTime sec\n")

    (tokensLP, idfModel, cvModel)
  }

  def predicateVectorize(data: RDD[(String,String, List[String])], cvModel: CountVectorizerModel, idf: IDFModel): RDD[(String,String,LabeledPoint)] = {
        val sc = data.context
        val sqlContext = SQLContext.getOrCreate(sc)
        import sqlContext.implicits._

        val tokenDF = data.toDF("pmid", "articleurl","tokens")
        var tokensLP : RDD[(String,String,LabeledPoint)] = null

        //转化为LabeledPoint
        var tokensDF = predicateToTFDF(tokenDF, cvModel)

        if (toTFIDF) {
          tokensLP = predicateToTFIDFLP(tokensDF, idf)
        }

        tokensLP
  }

  /**
    * 保存cvModel向量模型和idf向量
    *
    * @param vecModelPath 保存路径
    * @param cvModel      cvModel
    * @param idfModelPath 保存路径
    * @param idfModel     idfModel
    */
  def save(vecModelPath: String, idfModelPath: String, cvModel: CountVectorizerModel, idfModel: IDFModel, sc: SparkContext): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    fs.delete(new Path(vecModelPath), true)
    fs.delete(new Path(idfModelPath), true)
    cvModel.write.overwrite().save(vecModelPath)
    idfModel.write.overwrite().save(idfModelPath)
  }


  /**
    * 加载cvModel向量模型和idf向量
    *
    * @param vecModelPath 模型保存路径
    * @return (cvModel, idf)
    */
  def load(vecModelPath: String,idfModelPath: String,modelPath:String,sc: SparkContext): (CountVectorizerModel,IDFModel,LogisticRegressionModel) = {
    val cvModel = CountVectorizerModel.load(vecModelPath)
    val idfModel = IDFModel.load(idfModelPath)
    val lrModel = LogisticRegressionModel.load(sc,modelPath)
    (cvModel,idfModel,lrModel)
  }
}
