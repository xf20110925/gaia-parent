CREATE table wxMedia(
addtime                 bigint,
authinfo                string,
brief                   string,
headimage               string,
isauth                  bigint,
medianame               string,
pmid                    string,
qrcode                  string,
updatetime              bigint,
weixinid                string,
pubnuminperoid          bigint,
avgheadreadnuminperoid  bigint,
avgheadlikenuminperoid  bigint,
headreadpoints          array<struct<time:bigint,value:bigint>>,
secondreadpoints        array<struct<time:bigint,value:bigint>>,
thirdreadpoints         array<struct<time:bigint,value:bigint>>,
puboriginalarticlenuminperoid   bigint,
avgheadreadrateinperoid double,
avgheadlikerateinperoid double,
avgheadreadraterankinperoid     bigint,
avgheadlikeraterankinperoid     bigint,
pubarticlenuminperoid   bigint,
hotarticlenuminperoid   bigint,
totallikenuminperoid    bigint,
latestarticle           struct<time:string,value:string>,
mediahotwords           array<struct<score:string,token:string>>
)
PARTITIONED BY (time_date string)
STORED AS PARQUET
location "/user/gaia/wxMedia/";


CREATE table wbMedia(
itags                   array<String> ,
fansnum                 bigint,
postnum                 bigint ,
updatetime              bigint  ,
addtime                 bigint   ,
authinfo                string   ,
brief                   string   ,
gender                  bigint   ,
headimage               string   ,
isauth                  bigint   ,
location                string   ,
medianame               string   ,
pmid                    string   ,
regitertime             bigint   ,
pubnuminperoid          bigint   ,
avgcommentnuminperoid   bigint   ,
avgforwardnuminperoid   bigint   ,
avglikenuminperoid      bigint   ,
totalcommentpoints      array<struct<time:bigint,value:bigint>> ,
totalforwardpoints      array<struct<time:bigint,value:bigint>> ,
totallikepoints         array<struct<time:bigint,value:bigint>> ,
puboriginalarticlenuminperoid   bigint ,
avgcommentrateinperoid  double      ,
avgforwardrateinperoid  double       ,
avglikerateinperoid     double        ,
avgcommentraterankinperoid      bigint ,
avgforwardraterankinperoid      bigint  ,
avglikeraterankinperoid bigint          ,
latestarticle           struct<time:string,value:string>  ,
mediahotwords           array<struct<score:string,token:string>>
)
PARTITIONED BY (time_date string)
STORED AS PARQUET
location "/user/gaia/wbMedia/";
