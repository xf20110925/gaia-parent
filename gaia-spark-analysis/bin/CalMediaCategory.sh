#!/bin/bash

starttime=`date -d "today"   +"%Y-%m-%d %H:%m:%S"`


echo -e '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&  '$starttime' Start!!!!!' '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&\n\n'
version=3.8.4-SNAPSHOT

echo -e '-------------------- Cal (Wb/Wx) media to hive!!! --------------------\n'
spark-submit --master yarn-cluster --class com.ptb.gaia.analysis.category.CategoryMedia --executor-memory 512m hdfs:///user/gaia/jar/gaia-spark-analysis-${version}.jar "true" "true" "true" "true" "true" "true" "hdfs:///user/gaia/model" "hdfs:///user/gaia/category.txt"
echo -e '--------------------category ok End!!! --------------------\n'

echo -e '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&  '$endtime' End!!!!!' '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&\n'