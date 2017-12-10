package com.ptb.gaia.common.service

import java.util

import com.ptb.gaia.service.service.GWxMediaService
import org.apache.commons.collections.CollectionUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataTypes
import org.bson.Document

/**
  * Created by MengChen on 2017/2/8.
  */
class SaveWxToMongoImpl extends java.io.Serializable with IProcess {


  def saveWxDataToMongoLr(saveDf: DataFrame): Unit = {

    saveDf.withColumn("id", saveDf.col("id").cast(DataTypes.createArrayType(DataTypes.StringType, true))).foreachPartition(iter => {
      val wxMediaSave = new GWxMediaService()
      var list = new util.ArrayList[Document]()
      iter.foreach(row => {
        val document: Document = new Document
        document.put("pmid", row.getString(0))
        val categoryList = row.getList(1).asInstanceOf[util.List[java.lang.String]]
        document.put("tags",
          if (CollectionUtils.isEmpty(categoryList)) {
            new util.ArrayList[java.lang.String]()
          } else {
            categoryList
          }
        )
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


object SaveWxToMongoImpl {


}
