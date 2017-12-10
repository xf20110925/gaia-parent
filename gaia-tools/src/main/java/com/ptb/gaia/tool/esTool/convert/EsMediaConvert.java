package com.ptb.gaia.tool.esTool.convert;

import com.alibaba.fastjson.JSON;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.ptb.gaia.etl.flume.utils.Constant;
import com.ptb.gaia.index.bean.EsInsertData;
import com.ptb.gaia.index.EsMediaIndexImp;
import com.ptb.gaia.tool.esTool.data.MongoData;
import com.ptb.gaia.tool.esTool.util.MongoClientm;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by watson zhang on 16/6/30.
 */
public class EsMediaConvert {
    private static Logger logger = null;


    private static int switchType;
    private PropertiesConfiguration conf;
    public MongoData mongoData;
    public MongoClientm mongo;
    private static int intervalHour = 24;
    int faildNum;
    long lastTime;

    long weixinLastTime;
    long weiboLastTime;
    long liveLastTime;

    EsMediaIndexImp esMediaIndexImp;
    int batchSize;
    String[] hostArray;
    String clusterName;
    String indexName;

    private static String WEIXIN_COLL;
    private static String WEIBO_COLL;
    private static String LIVE_COLL;

    private static String WEIXIN_INDEX = "weixinMedia";
    private static String WEIBO_INDEX = "weiboMedia";
    private static String LIVE_INDEX = "liveMedia";

    public EsMediaConvert(){
        logger = LoggerFactory.getLogger(EsMediaConvert.class);
        try {
            conf = new PropertiesConfiguration("ptb.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        hostArray = conf.getStringArray("gaia.es.hosts");
        clusterName = conf.getString("gaia.es.clusterName", "ptbes");
        lastTime = conf.getLong("gaia.convert.lastTime", 0);
        indexName = conf.getString("gaia.es.media.index", "media");
        weixinLastTime = lastTime;
        weiboLastTime = lastTime;
        intervalHour = conf.getInt("gaia.convert.intervalHour", 24);
        WEIXIN_COLL = conf.getString("gaia.tools.media.mongo.wxcoll", "wxMedia");
        WEIBO_COLL = conf.getString("gaia.tools.media.mongo.wbcoll", "wbMedia");
        LIVE_COLL = conf.getString("gaia.tools.media.mongo.livecoll", "liveMedia");
        batchSize = conf.getInt("gaia.es.batchSize", 3000);
        mongo = new MongoClientm();
        mongoData = mongo.getMongoData();

        esMediaIndexImp = new EsMediaIndexImp();
    }

    public void logConfInfo(String coll, String indexType){
        logger.info("mongo server:\n host: {}\n port: {}\n db: {}\n coll: {}",
                mongoData.getMongoHostList(),
                mongoData.getMongoPort(),
                mongoData.getMongoDataBase(),
                coll);
        logger.info("es server:\n hosts:{}\n cluster: {}\n indexName: {}\n indexType: {}\n batchSize: {}\n intervalTime: {}\n lastTime: {}",
                hostArray,
                clusterName,
                indexName,
                indexType,
                batchSize,
                intervalHour,
                lastTime);
    }


    public void convertMedia(int type){

        MongoCollection<Document> collection = null;
        Bson filters = null;
        String indexType = null;
        if(type == Constant.P_WEIXIN){
            collection = mongo.mongoClient.getDatabase(mongo.getMongoData().getMongoDataBase()).getCollection(WEIXIN_COLL);
            filters = Filters.and(Filters.gt("updateTime", weixinLastTime));
            indexType = WEIXIN_INDEX;
            this.logConfInfo(WEIXIN_COLL, indexType);
        }else if(type == Constant.P_WEIBO){
            collection = mongo.mongoClient.getDatabase(mongo.getMongoData().getMongoDataBase()).getCollection(WEIBO_COLL);
            filters = Filters.and(Filters.gt("updateTime", weiboLastTime));
            indexType = WEIBO_INDEX;
            this.logConfInfo(WEIBO_COLL, indexType);
        }else if (type == Constant.P_LIVE){
            collection = mongo.mongoClient.getDatabase(mongo.getMongoData().getMongoDataBase()).getCollection(LIVE_COLL);
            filters = Filters.and(Filters.gt("updateTime", liveLastTime));
            indexType = LIVE_INDEX;
            this.logConfInfo(LIVE_COLL, indexType);
        }
        logger.info("get coll success");
        long currentNum = 0;
        long internalTime = 60000;
        int timeMinute = 0;
        long lastTime = 0;

        FindIterable findIterable = collection.find();
        long mediaCount = collection.count();
        MongoCursor cursor = findIterable.iterator();
        long startTime = System.currentTimeMillis();
        if(!cursor.hasNext()){
            logger.info("there no meida!");
            return;
        }
        logger.error("current num: {}, total num: {}", currentNum, mediaCount);

        int i = 0;
        logger.error("start work...");
        EsInsertData esInsertData = new EsInsertData();
        Document doc = null;
        int ret = 0;
        while (cursor.hasNext()){
            try {
                doc = (Document) cursor.next();
                if (doc != null && doc.getLong("updateTime") != null){
                    long addTime = doc.getLong("updateTime");
                    if(addTime > lastTime){
                        lastTime = addTime;
                    }
                }

                esInsertData.setId(doc.getString("_id"));
                esInsertData.setJsonObject(JSON.parseObject(doc.toJson()));
                if (type == Constant.P_WEIXIN){
                    ret = esMediaIndexImp.upsertWxMediaFromMongo(esInsertData);
                }else if (type ==Constant.P_WEIBO){
                    ret = esMediaIndexImp.upsertWbMediaFromMongo(esInsertData);
                }else if (type == Constant.P_LIVE){
                    ret = esMediaIndexImp.insertLiveMediaFromMongo(esInsertData);
                }
                if (ret < 0){
                    faildNum++;
                }

                i++;
                if (i >= batchSize){
                    esMediaIndexImp.flushEsBulk();
                    long endTime = System.currentTimeMillis();
                    currentNum += i;
                    i = 0;
                    if ((endTime - startTime)/internalTime  > timeMinute){
                        timeMinute++;
                        logger.error("current num: {} faildNum:{}, total num: {}, lastTime: {}", currentNum, faildNum, mediaCount, lastTime);
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
                logger.error("start error!");
                System.out.println(doc);
                faildNum += i;
                i = 0;
            }
        }
        currentNum += i;
        if (lastTime != 0){
            if(type == Constant.P_WEIXIN){
                weixinLastTime = lastTime;
            }else if(type == Constant.P_WEIBO){
                weiboLastTime = lastTime;
            }else if (type == Constant.P_LIVE){
                liveLastTime = lastTime;
            }
        }
        esMediaIndexImp.flushEsBulk();
        logger.error("successful end...");
        logger.error("faild num: {}", faildNum);
        logger.error("end num: {}, total num: {}", currentNum, mediaCount);
        logger.error("last time: {}", lastTime);
    }

    public void convertTask(){
        this.convertMedia(Constant.P_WEIXIN);
        this.convertMedia(Constant.P_WEIBO);
        this.convertMedia(Constant.P_LIVE);
        return;
    }

    public static void startTask(int switchOne, int type){
        switchType = switchOne;
        EsMediaConvert esMediaConvert = new EsMediaConvert();
        if(switchOne == 0){
            if (type != Constant.P_WEIXIN && type != Constant.P_WEIBO && type != Constant.P_LIVE){
                logger.error("the args importing to es need 1 or 2!");
                return;
            }
            esMediaConvert.convertMedia(type);
        } else if (switchOne == 1){
            while (true){
                esMediaConvert.convertTask();
                try {
                    Thread.sleep(TimeUnit.MILLISECONDS.toHours(intervalHour));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args){
        EsMediaConvert.startTask(0, 2);
    }
}
