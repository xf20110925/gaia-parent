import com.ptb.gaia.analysis.calculation.MediaChannelDetailHandle
import com.ptb.gaia.analysis.category.CategoryMedia

/**
 * Created by eric on 16/7/27.
 */
object TestCategory {
  def main(args: Array[String]) {
/*    val Array(doToken,doGenTokenDic,doTakeSample,doTrain,doCategory,doPredicate,modelPath) = Array(
      "true","true","true","true","true","true","/Volumes/Pan/model"
    )*/

//    val Array(doToken,doGenTokenDic,doTakeSample,doTrain,doPredicate,doPremiumCategory,modelPath,knlibPath) = Array(
//      "false","false","false","false","false","true","/Volumes/Pan/model","file:///Users/eric/ptbProject/gaia-parent/gaia-spark-analysis/src/main/resources/category.txt"
//    )
//    CategoryMedia.main(Array(doToken,doGenTokenDic,doTakeSample,doTrain,doPredicate,doPremiumCategory,modelPath,knlibPath) )
    MediaChannelDetailHandle.main(Array())
  }
}
