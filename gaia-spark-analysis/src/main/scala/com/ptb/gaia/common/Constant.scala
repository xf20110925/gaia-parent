package com.ptb.gaia.common

/**
  * Created by MengChen on 2016/7/22.
  */
class Constant {


}

object Constant {
  /*
  Character set in filter (Chinese And English)
   */
  val charRegEx = "[?《》`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]"

  /*
  update_time 增加15天时间作为 update_time 的结束时间
   */
  val update_Add_Fifteen_Day: Int = 15

  /*
  某个月份的0点0分0秒  需要价格后缀  postTime
   */
  val pTime_Suffix_0Time: String = "000000"

  /*
某个月份的23点59分59秒  需要价格后缀 postTime
 */
  val pTime_suffix_23Time: String = "235959"

  /*
某个月份的0点  需要价格后缀  updateTime
 */
  val uTime_Suffix_0Time: String = "00"

  /*
某个月份的23点  需要价格后缀 updateTime
 */
  val uTime_suffix_23Time: String = "23"

  /*
  微信平台标识
 */
  val wei_Xin_Type: Integer = 1

  /*
微博平台标识
*/
  val wei_Bo_Type: Integer = 2

  /*
7天存储方式
*/
  val seven_Save_Type: Integer = 7

  /*
30天存储方式
*/
  val thirty_Save_Type: Integer = 30

  /*
14天存储方式
*/
  val fourteen_Save_Type: Integer = 14

  /*
  redis 失效时间 针对和名单设置的
   */

  val expire_Time = 172800


  /*
  LR 词频次数限制
   */
  val minDocFreq = 1


}
