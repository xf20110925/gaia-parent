
cat */send.log*|grep article_basic|sed 's/\/\/weibo.com/\/\/m.weibo.cn/g'|sed 's/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}   //g' > article_basic.txt
cat */send.log*|grep article_spread|sed 's/\/\/weibo.com/\/\/m.weibo.cn/g'|sed 's/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}   //g' > article_spread.txt

cat article_basic.txt |./bin/tools.sh -send_msg article_basic_info
cat article_spread.txt |./bin/tools.sh -send_msg article_spread_info