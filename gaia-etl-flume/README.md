gaia-flume
========================

简介
--------------------------

1. 负责从KAFKA中接受uranus放来的消息并进行存储操作


运行需求
-------------------------

1. centos6+
2. java 8+


安装手册
-------------------------
0. 第一次安装需初始化mongodb数据库
   建立数据库名  gaia2
  
   文章表数据库需要手动建立collection,
   微博文章大小并限制其集合max:文章入库1000万条,保持一个月,3亿条
   微信文章大小并限制其大小max:文章入库每天300万条,保持一个月是1亿条
  
        db.createCollection("wxArticle", {max: 100000000})
        db.createCollection("wbArticle", {max: 100000000})
        db.createCollection("wxMedia")
        db.createCollection("wbMedia")
    
   媒体表的分片键为_id即可
   文章表的分片键为pmid即可
   
   #建立索引
   wxArticle updateTime pmid
   wbArticle updateTime pmid 
   wxMedia   updateTime
   wbMedia   updateTime
   
      db.wxArticle.createIndex({"updateTime":-1})
      db.wxArticle.createIndex({"pmid":1})
      db.wbArticle.createIndex({"updateTime":-1})
      db.wbArticle.createIndex({"pmid":1})
      db.wxMedia.createIndex({"updateTime":-1})
      db.wbMedia.createIndex({"updateTime":-1})
   
      db.runCommand({ shardcollection: "gaia2.wxArticle", key: { pmid:1 }})
      db.runCommand({ shardcollection: "gaia2.wbArticle", key: { pmid:1 }})
      db.runCommand({ shardcollection: "gaia2.wbMedia", key: { _id:1 }})
      db.runCommand({ shardcollection: "gaia2.wxMedia", key: { _id:1 }})
      

1. 修改配置文件
   
      
        gaia.properties
   
   
导入数据
===========================
###还原线下数据

        mongorestore -h 192.168.5.200 -d gaia2 --noIndexRestore -c wxArticle gaia2db/test/wxArticle.bson
        mongorestore -h 192.168.5.200 -d gaia2 --noIndexRestore -c wbArticle gaia2db/test/wbArticle.bson
        mongorestore -h 192.168.5.200 -d gaia2 --noIndexRestore -c wxArticle gaia2db/test/wxArticle.bson
        mongorestore -h 192.168.5.200 -d gaia2 --noIndexRestore -c wbArticle gaia2db/test/wbArticle.bson
        mongorestore -h 192.168.5.200 -d gaia2 --noIndexRestore -c wxMedia gaia2db/test/wxMedia.bson
        mongorestore -h 192.168.5.200 -d gaia2 --noIndexRestore -c wbMedia gaia2db/test/wbMedia.bson

###重发线上
        cat */send.log*|grep article_basic|sed 's/\/\/weibo.com/\/\/m.weibo.cn/g'|sed 's/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}   //g' > article_basic.txt
        cat */send.log*|grep article_spread|sed 's/\/\/weibo.com/\/\/m.weibo.cn/g'|sed 's/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}   //g' > article_spread.txt

        cat article_basic.txt |./bin/tools.sh -send_msg article_basic_info
        cat article_spread.txt |./bin/tools.sh -send_msg article_spread_info
        
        
        
    


