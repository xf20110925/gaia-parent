package com.ptb.gaia.process

import _root_.sun.misc.Signal
import com.alibaba.fastjson.JSON
import com.mongodb.BasicDBObject
import com.ptb.gaia.tools.Tools
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import com.stratio.datasource.mongodb.partitioner.MongodbPartitioner
import com.stratio.datasource.mongodb.query.RawFilter
import com.stratio.datasource.mongodb.rdd.MongodbRDD
import com.stratio.datasource.mongodb.schema.MongodbRowConverter
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataType, StructType}

/**
  * C
  * /*    //判断一下输入参数的个数是否规范
  * if (schemaFile.size != 5) {
  * println("Please Input File Four Args:")
  * println("Frist Cell : MongoDB StructType Json") //0
  * println("Second Cell : Require Column") //2
  * println("Third Cell : DataFrame Temp Table") //3
  * println("Fourth Cell : Hive SQL") //4
  * println("Fifth Cell : MongoDB Config") //5
  * *
  * System.exit(1)
  * *
  * }*/
  */
object MongoDb2HiveFillData {

  def MongodbToHive(args: Array[String], sc: SparkContext): Unit = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val hiveContext = new HiveContext(sc)

    val Array(schemaFilePath, queryCondition, splitmin,splitmax) : Array[String] = args

    val schemaFile = sc.textFile(schemaFilePath).collect()

    val Array(schemaStruct, requireFields, tempTableName, sql, mongodbConfig) = schemaFile

    //MongoDB Config
    val json = JSON.parseObject(mongodbConfig)

    //mongodb host
    val hostList = json.get("host").toString.split(",").toList
    var option = {
      Map(SplitKeyType -> json.get("splitKeyType").toString,
        SplitKey -> json.get("splitKey").toString,
        Host -> hostList,
        Database -> json.get("database").toString,
        CursorBatchSize -> Integer.valueOf(json.get("cursorBatchSize").toString),
        Collection -> json.get("collection").toString,
        SplitSize -> json.get("splitSize").toString,
        WriteConcern -> "normal") ++ (if(splitmax != null) Map(SplitKeyMax -> splitmax,SplitKeyMin->splitmin) else Map.empty[String,Object])
    }

    val readConfig = MongodbConfigBuilder(option).build()

    //Require Column
    val requireFieldArrays = requireFields.split(",")

    //实现读取配置文件生成schema    MongoDB StructType Json
    val schema = DataType.fromJson(schemaStruct).asInstanceOf[StructType]

    //获取mongdb查询表达式   MongoDB Expr

    val dBObject = com.mongodb.util.JSON.parse(queryCondition).asInstanceOf[BasicDBObject]

    val mongodbrdd = new MongodbRDD(sqlContext, readConfig, new MongodbPartitioner(readConfig), requireFieldArrays, RawFilter(dBObject))

    println("!!!!!!!!!!")

    if(mongodbrdd.isEmpty()){
      sc.stop()
      System.gc()
      Signal.raise(new Signal("INT"))
    }

    println("##########")

    // DataFrame Temp Table
    hiveContext.createDataFrame(MongodbRowConverter.asRow(schema, mongodbrdd), schema).repartition(1).registerTempTable(tempTableName)

    println("~~~~~~~~~~")

    //HiveContext 将临时表导入到Hive   Hive SQL
    hiveContext.sql("set hive.exec.dynamic.partition=true;")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nostrick;")

    hiveContext.sql(sql)

    println("&&&&&&&&&&")

    sc.stop()

    System.gc()

    Signal.raise(new Signal("INT"))

  }

  def main(args: Array[String]) {

    new Tools().runJob(MongodbToHive, args, new Tools().getInitSC("SparkMongo2Hive"))

  }

}
