#!/bin/bash

title_time=`date -d today +"%Y-%m-%d"`

echo -e '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&  '$title_time' start!!!!!' '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&\n'

##获取当前时间
#time5=`date -d today +"%Y-%m-%d %H":00:00`
#time5=`date -d "-1 hour" "+%Y-%m-%d "23:59:59`
time5=`date -d "-1 hour" "+%Y-%m-%d %H":59:59`

last_time=`date -d "$time5" +%s`000



##获取前一小时的时间
#time6=`date -d "-3 hour" "+%Y-%m-%d %H":00:00`
#time6=`date -d "yesterday" "+%Y-%m-%d "00:00:00`
time6=`date -d "-1 hour" "+%Y-%m-%d %H":00:00`

pre_time=`date -d "$time6" +%s`000


##拼接monggodb查询表达式
#mongodbexpr="{ \"addTime\" : { \"\$lte\" : { \"\$date\" : \"$last_time\" },\"\$gte\" : { \"\$date\" : \"$pre_time\" } } }"
mongodbexpr="{}"



echo -e '现在提取Mongodb to Hive的数据时间 ：'$time6' to ' $time5 '\n'

##开始读取数据 wbArticle 表的数据
echo -e '*********开始读取数据 wbArticle 表的数据*********\n'
#--conf spark.sql.shuffle.partitions=100
##提交 Spark Job任务
spark-submit --master yarn-cluster --class com.ptb.gaia.process.MongoDb2Hive --conf spark.sql.shuffle.partitions=100  --conf spark.shuffle.consolidateFiles=true --conf spark.shuffle.compress=true --conf spark.memory.useLegacyMode=true hdfs://hadoop_namenode1_ip:8020/user/gaia/jar/gaia-sparketl-current_module_version.jar /user/gaia/config/wbArticle.txt "$mongodbexpr" "$pre_time" "$last_time"
echo -e '\n'
echo -e '*********结束读取数据 wbArticle 表的数据*********\n\n'


##开始读取数据 wxArticle 表的数据
echo -e '*********开始读取数据 wxArticle 表的数据*********\n'

##提交 Spark Job任务
spark-submit --master yarn-cluster --class com.ptb.gaia.process.MongoDb2Hive --conf spark.sql.shuffle.partitions=100 --conf spark.shuffle.consolidateFiles=true --conf spark.shuffle.compress=true --conf spark.memory.useLegacyMode=true hdfs://hadoop_namenode1_ip:8020/user/gaia/jar/gaia-sparketl-current_module_version.jar /user/gaia/config/wxArticle.txt "$mongodbexpr" "$pre_time" "$last_time"

echo -e '\n'
echo -e '*********结束读取数据 wxArticle 表的数据*********\n\n'




echo -e '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&  '$title_time' End!!!!!' '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&'