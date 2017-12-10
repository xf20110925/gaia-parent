package com.ptb.gaia.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

/**
  * Created by MengChen on 2016/5/30.
  */
class DataUtil {


  /*
    时间戳转时间
   */
  def timeStamp2Date(seconds: String, format: String): String = {

    var format1 = format
    var seconds1 = seconds

    if (seconds1 == null || seconds1.isEmpty || (seconds1 == "null")) {
      return ""
    }
    if (format1 == null || format1.isEmpty) {
      format1 = "yyyy-MM-dd HH:mm:ss"
    }
    val sdf: SimpleDateFormat = new SimpleDateFormat(format1, Locale.CHINA)

    if(seconds.length!=13){
      return sdf.format(new Date(java.lang.Long.valueOf(seconds1 + "000")))
    }

    return sdf.format(new Date(java.lang.Long.valueOf(seconds1)))

  }

  /*
    时间转时间戳
   */
  def date2TimeStamp(date_str: String): String = {

    var date_str1 = date_str

    try {
      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.CHINA)
      return String.valueOf(sdf.parse(date_str1).getTime / 1000)
    }
    catch {
      case e: Exception => {
        e.printStackTrace
      }
    }
    return ""

  }

  /*
  传进的时间 增加多少天
  1、时间 例如：20160930  2、指定多少天 例如：2
  返回： 20161002
 */
  def addTime(date_str: String, day: Integer): String = {

    val cal: Calendar = Calendar.getInstance(Locale.CHINA)
    val time = new SimpleDateFormat("yyyyMMdd", Locale.CHINA).parse(date_str)

    cal.setTime(time)
    cal.add(Calendar.DATE, day)

    val date = cal.getTime();

    new SimpleDateFormat("yyyyMMdd", Locale.CHINA).format(date)
  }

  /*
  判断是否为合法日期
*/
  def isValidDate(date_str: String): Boolean = {

    try {
      val format = new SimpleDateFormat("yyyyMMdd");
      format.setLenient(false);
      format.parse(date_str);
      true
    } catch {
      case ex: Exception => {
        false
      }
    }

  }


}
