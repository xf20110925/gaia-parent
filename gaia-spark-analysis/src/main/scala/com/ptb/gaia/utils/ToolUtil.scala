package com.ptb.gaia.utils

import java.util.regex.{Matcher, Pattern}

import com.ptb.gaia.common.Constant
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType

/**
  * Created by MengChen on 2016/7/21.
  */
class ToolUtil extends Serializable{


  /*
  transform Old Type to New Type
 */

  def castColumnTo(df: DataFrame, cn: String, tpe: DataType): DataFrame = {
    df.withColumn(cn, df(cn).cast(tpe))
  }


  /*
   Whether to include Chinese and English character sets
   */

  def includeChinaAndEngChar(str: String): Boolean = {

    val p: Pattern = Pattern.compile(Constant.charRegEx)
    val m: Matcher = p.matcher(str)
//    if(m.find()==true){
//      return 1
//    }else{
//      return 0
//    }
    m.find()
  }

}

object ToolUtil{

  def apply() = new ToolUtil

}



