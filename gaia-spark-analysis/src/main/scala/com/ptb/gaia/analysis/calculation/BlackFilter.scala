package com.ptb.gaia.analysis.calculation

import com.ptb.gaia.common.Constant
import com.ptb.gaia.service.utils.JedisUtil
import com.ptb.gaia.utils.{DataUtil, SetUpUtil}
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import sun.misc.Signal

/**
  * Created by MengChen on 2016/11/8.
  * 黑名单过滤程序 用于微信爬虫过滤使用  pmid列表 暂定一天执行一次   redis留出白名单
  */
class BlackFilter {

}

object BlackFilter {

  def entry(args: Array[String], sc: SparkContext): Unit = {

    val hsc = new HiveContext(sc)

    //endTime 格式 20161108   pre_OneMonth 格式 1480000000000
    val Array(endTime, pre_Month) = args

    //获取两个月时间的结束时间
    val time_End: Integer = Integer.valueOf(endTime.concat(Constant.uTime_suffix_23Time))
    //获取当天时间的0点
    val todayTime_Start: Integer = Integer.valueOf(new DataUtil().addTime(endTime, -5).concat(Constant.uTime_Suffix_0Time))
    //获取两个月时间的开始时间
    val time_Start: Integer = Integer.valueOf(new DataUtil().addTime(endTime, -10).concat(Constant.uTime_Suffix_0Time))
    //获取两个月时间的未发文截止时间
    val time_Month: Long = java.lang.Long.valueOf(pre_Month)

    hsc.sql("use gaia")

    val sqlStr1 = "select pmid from gaia.wxmedia where time_date>=2016111000 group by pmid union all select pmid from gaia.wbmedia where time_date>=2016111000 group by pmid"

    //循环写入redis
    hsc.sql(sqlStr1).foreachPartition(iter => {
      iter.foreach(row => {
        JedisUtil.set(row.getString(0).getBytes, "1".getBytes)
        })
      }
    )

    //现在redis 是全量数据
    ///这个sql  需要重新写  条件为  updateTime不为空 latestarticle不为空  并且 latestarticle.time 在两个月之内没有发文从redis去掉  剩下的就是 白名单数据
    val sqlStr = "SELECT b.pmid FROM (SELECT a.pmid,a.latestarticle.time AS time,row_number()over(partition by a.pmid ORDER BY a.latestarticle.time desc) rank FROM wxmedia a WHERE a.latestarticle.time is NOT null AND a.time_date>=" + time_Start + " AND a.time_date<=" + time_End + " ) b WHERE rank=1 AND time<=" + time_Month + " "

    //循环写入redis
//    hsc.sql(sqlStr).foreachPartition(iter => {
//      iter.foreach(row => {
//          JedisUtil.del(row.getString(0).getBytes)
//      })
//    }
//    )

    Signal.raise(new Signal("INT"))
  }


  def main(args: Array[String]) {
    new SetUpUtil().runJob(entry, args, new SetUpUtil().getInitSC("BlackFilter"))
  }


}
