#!/bin/bash


title_time=`date -d today +"%Y-%m-%d"`

echo -e '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&  '$title_time' start!!!!!' '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&\n'

##获取当前时间
#time5=`date -d today +"%Y-%m-%d %H":00:00`
time5=`date -d "-1 hour" "+%Y-%m-%d %H":59:59`
#time1=`echo $time5 | cut -c 1-10`
#time2=`echo $time5 | cut -c 12-19`
#last_time=${time1}T${time2}.999Z
last_time=`date -d "$time5" +%s`000



##获取前一小时的时间   
time6=`date -d "-1 hour" "+%Y-%m-%d %H":00:00`
#time3=`echo $time6 | cut -c 1-10`
#time4=`echo $time6 | cut -c 12-19`
#pre_time=${time3}T${time4}.000Z
pre_time=`date -d "$time6" +%s`000



##拼接monggodb查询表达式
#mongodbexpr="{ \"addTime\" : { \"\$lte\" : { \"\$date\" : \"$last_time\" },\"\$gte\" : { \"\$date\" : \"$pre_time\" } } }"
mongodbexpr="{ \"addTime\" : { \"\$lte\":$last_time,\"\$gte\":$pre_time } }"
#mongodbexpr="{ \"addTime\" : { \"\$lte\":$last_time,\"\$gte\":$pre_time } }"
mongodbexpr="{ }"
etl_version=gaia-sparketl-3.8.4-SNAPSHOT.jar

echo -e '现在提取Mongodb to Hive的数据时间 ：'$time6' to ' $time5 '\n'

##开始读取数据 wbMedia 表的数据
echo -e '*********开始读取数据 wbMedia 表的数据*********\n'
#--conf spark.sql.shuffle.partitions=100
##提交 Spark Job任务
spark-submit --master yarn-cluster --class com.ptb.gaia.process.MongoDb2Hive --conf spark.sql.shuffle.partitions=100 --conf spark.shuffle.consolidateFiles=true --conf spark.shuffle.compress=true --conf spark.memory.useLegacyMode=true hdfs:///user/gaia/jar/${etl_version} /user/gaia/config/wbMedia.txt "$mongodbexpr" "$pre_time" "$last_time"

echo -e '\n'
echo -e '*********结束读取数据 wbMedia 表的数据*********\n\n'


##开始读取数据 wxMedia 表的数据
echo -e '*********开始读取数据 wxMedia 表的数据*********\n'
#--conf spark.sql.shuffle.partitions=100
##提交 Spark Job任务
spark-submit --master yarn-cluster --class com.ptb.gaia.process.MongoDb2Hive --conf spark.sql.shuffle.partitions=100 --conf spark.shuffle.consolidateFiles=true --conf spark.shuffle.compress=true --conf spark.memory.useLegacyMode=true hdfs:///user/gaia/jar/${etl_version}  /user/gaia/config/wxMedia.txt "$mongodbexpr" "$pre_time" "$last_time"

echo -e '\n'
echo -e '*********结束读取数据 wxMedia 表的数据*********\n\n'



