#!/usr/bin/env bash
##$1为DHFS /user后面目录的名字
HOME=$(cd "$(dirname "$0")/../"; pwd)
OUTPUT_DIR1=`pwd`
OUTPUT_DIR=$OUTPUT_DIR1/dist
path=$1
hadoop dfs -mkdir /user/${path}/config
hadoop dfs -mkdir /user/${path}/jar
hadoop dfs -mkdir /user/${path}/sh

filename=`ls ${OUTPUT_DIR} | grep *spark*an*jar*`
filename2=`ls ${OUTPUT_DIR} | ls |grep *spark*etl*jar*`

cd $OUTPUT_DIR

hadoop dfs -rmr /user/${path}/jar/${filename}
hadoop dfs -rmr /user/${path}/jar/${filename2}
hadoop dfs -rmr /user/${path}/config/*
hadoop dfs -rmr /user/${path}/sh/*

hadoop dfs -put ./${filename} /user/${path}/jar/
hadoop dfs -put ./${filename2} /user/${path}/jar/

bin_path=$OUTPUT_DIR1/bin
config_path=$OUTPUT_DIR1/config

cd $bin_path
hadoop dfs -put ./* /user/${path}/sh/

cd $config_path
hadoop dfs -put ./* /user/${path}/config/