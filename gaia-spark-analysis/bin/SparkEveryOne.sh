#!/bin/bash

starttime=`date -d "today"   +"%Y-%m-%d %H:%m:%S"`


echo -e '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&  '$starttime' Start!!!!!' '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&\n\n'


echo -e '-------------------- WeiboAnalysis Start!!! --------------------\n'
spark-submit --master yarn-cluster --class com.ptb.gaia.analysis.weibo.WeiboAnalysis --executor-memory 5g hdfs://hadoop_namenode1_ip:8020/user/gaia/jar/gaia-spark-analysis-current_module_version.jar
echo -e '-------------------- WeiboAnalysis End!!! --------------------\n'


echo -e '-------------------- WeixinAnalysis Start!!! --------------------\n'
spark-submit --master yarn-cluster --class com.ptb.gaia.analysis.weixin.WeixinAnalysis hdfs://hadoop_namenode1_ip:8020/user/gaia/jar/gaia-spark-analysis-current_module_version.jar
echo -e '-------------------- WeixinAnalysis End!!! --------------------\n'


echo -e '-------------------- WeiboMediaHotToken Start!!! --------------------\n'
spark-submit --master yarn-cluster --class com.ptb.gaia.analysis.weibo.WeiboMediaHotToken hdfs://hadoop_namenode1_ip:8020/user/gaia/jar/gaia-spark-analysis-current_module_version.jar
echo -e '-------------------- WeiboMediaHotToken End!!! --------------------\n'

echo -e '-------------------- WeixinMediaHotToken Start!!! --------------------\n'
spark-submit --master yarn-cluster --class com.ptb.gaia.analysis.weixin.WeixinMediaHotToken hdfs://hadoop_namenode1_ip:8020/user/gaia/jar/gaia-spark-analysis-current_module_version.jar
echo -e '-------------------- WeixinMediaHotToken End!!! --------------------\n'


endtime=`date -d "today"   +"%Y-%m-%d %H:%m:%S"`


echo -e '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&  '$endtime' End!!!!!' '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&\n'