#!/usr/bin/env bash

starttime=`date -d "today"   +"%Y-%m-%d %H:%m:%S"`


echo -e '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&  '$starttime' Start!!!!!' '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&\n\n'


echo -e '-------------------- WeiboAnalysis Start!!! --------------------\n'
spark-submit --master yarn-cluster --class com.ptb.gaia.analysis.zeus.UMAExport hdfs://hadoop_namenode1_ip:8020/user/gaia/jar/gaia-spark-analysis-current_module_version.jar
echo -e '-------------------- WeiboAnalysis End!!! --------------------\n'


endtime=`date -d "today"   +"%Y-%m-%d %H:%m:%S"`


echo -e '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&  '$endtime' End!!!!!' '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&\n'
