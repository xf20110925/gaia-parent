#!/bin/bash

if [ $# < 3 ]
then
    echo "need three arguments:\n\tfirst: host\n\tsecond: port\n\tthird: indexName"
    exit -1
fi
#更新1.1.0：
host=$1
port=$2
indexName=$3

echo $host
echo $port
echo $indexName

url=http://$host:$port

curl -XPUT $url/$indexName -d '{
  "settings": {
    "number_of_shards":   5,
    "number_of_replicas": 1
  }
}'

curl -XPOST $url/$indexName/weixinMedia/_mapping?pretty -d'
{
    "weixinMedia": {
            "_all": {
            "analyzer": "ik_max_word",
            "search_analyzer": "ik_max_word",
            "term_vector": "no",
            "store": "false"
        },
        "properties": {
            "timeStamp": {
                "type": "long"
            },
            "pmid": {
                "type": "string",
                "index":  "not_analyzed"
            },
            "mediaName": {
                "type": "string",
                "store": "no",
                "term_vector": "with_positions_offsets",
                "analyzer": "ik_max_word",
                "search_analyzer": "ik_max_word",
                "include_in_all": "true",
                "boost": 2
            },
            "mediaNameLen": {
                "type": "integer"
            },
            "authInfo": {
                "type": "string",
                "index": "not_analyzed",
                "boost": 0.5
            },
            "brief": {
                "type": "string",
                "index": "not_analyzed",
                "ignore_above": 256,
                "boost": 0.5
            },
            "isAuth": {
                "type": "integer"
            },
            "tags": {
                "type": "string",
                "index": "not_analyzed"
            },
            "mediaScore": {
                "type": "double"
            }
        }
    }
}'

curl -XPOST $url/$indexName/weiboMedia/_mapping?pretty -d'
{
    "weiboMedia": {
            "_all": {
            "analyzer": "ik_max_word",
            "search_analyzer": "ik_max_word",
            "term_vector": "no",
            "store": "false"
        },
        "properties": {
            "timeStamp": {
                "type": "long"
            },
            "pmid": {
                "type": "string",
                "index":  "not_analyzed"
            },
            "mediaName": {
                "type": "string",
                "store": "no",
                "term_vector": "with_positions_offsets",
                "analyzer": "ik_max_word",
                "search_analyzer": "ik_max_word",
                "boost": 2
            },
            "fansNum": {
                "type": "integer"
            },
            "logFans": {
                "type": "double"
            },
            "authInfo": {
                "type": "string",
                "index": "not_analyzed",
                "boost": 0.5
            },
            "brief": {
                "type": "string",
                "index": "not_analyzed",
                "ignore_above": 256,
                "boost": 0.5
            },
            "isAuth": {
                "type": "integer"
            },
            "tags": {
                "type": "string",
                "index": "not_analyzed"
            },
            "mediaScore": {
                "type": "double"
            }
        }
    }
}'
