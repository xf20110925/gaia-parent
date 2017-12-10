gaia-sparketl
=============

简介
---
1.对微博文章数据的导入Hive
2.对微信文章数据的导入Hive


运行需求
----
1.centos6.5
2.java 8+


安装手册
----
1.在HDFS文件系统上指定目录创建子目录:
/config : 程序启动需要的配置文件,里面配置了表需要的基本信息
/jar: 使用maven打包(gaia-sparketl-111.jar),将jar包上传到这个目录下面,需要的程序在此jar包里.
/sh : 通过脚本控制spark程序的启动顺序

相关的命令:
创建文件:hadoop dfs -mkdir /指定主目录/config

上传文件:hadoop dfs -put ./本地指定文件 /指定主目录/sh

删除文件:hadoop dfs -rmr 指定文件目录

赋予权限:hadoop dfs -chmod 777 指定目录

2.通过hue配置自动化执行计划