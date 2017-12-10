package com.ptb.gaia.tool.esTool.convert;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.ptb.gaia.tool.esTool.data.EsData;
import com.ptb.gaia.tool.esTool.data.MongoData;
import com.ptb.gaia.tool.esTool.util.EsTransportClient;
import com.ptb.gaia.tool.esTool.util.MongoClientm;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by watson zhang on 2016/11/2.
 */
public class ConvertMedia implements Runnable{

    private static Logger logger = LoggerFactory.getLogger(ConvertMedia.class);

    private static String WEIXIN_TYPE = "wxMedia";
    private static String WEIBO_TYPE = "wbMedia";

    private static String WEIXIN_INDEX = "weixinMedia";
    private static String WEIBO_INDEX = "weiboMedia";

    private PropertiesConfiguration conf;

    long weixinLastTime;
    long weiboLastTime;

    private static int intervalHour = 24;
    long lastTime;
    public MongoData mongoData;
    public MongoClientm mongo;
    public EsTransportClient esClient;
    public EsData esData;
    long mediaCount;
    int faildNum;

    String mongoLock = "mongoLock";
    String esLock = "esLock";
    int mediaType;
    int switchType;

    private MongoCursor mongoCursor;

    public ConvertMedia(int mediaType, int switchType){
        try {
            conf = new PropertiesConfiguration("ptb.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        lastTime = conf.getLong("gaia.convert.lastTime", 0);
        intervalHour = conf.getInt("gaia.convert.intervalHour", 24);
        mongo = new MongoClientm();
        mongoData = mongo.getMongoData();

        esClient = new EsTransportClient();
        esData = esClient.getEsData();
        esData.setIndexName(conf.getString("gaia.es.media.indexName", "media"));
        mongoCursor = this.getMongoCursor(mediaType);
        this.mediaType = mediaType;
        this.switchType = switchType;
    }

    public void logConfInfo(String coll, String indexType){
        logger.info("mongo server:\n host: {}\n port: {}\n db: {}\n coll: {}",
                mongoData.getMongoHost(),
                mongoData.getMongoPort(),
                mongoData.getMongoDataBase(),
                coll);
        logger.info("es server:\n hosts:{}\n cluster: {}\n indexName: {}\n indexType: {}\n batchSize: {}\n intervalTime: {}\n lastTime: {}",
                esClient.getEsData().getHostNames(),
                esClient.getEsData().getClusterName(),
                esData.getIndexName(),
                indexType,
                esData.getBatchSize(),
                intervalHour,
                lastTime);
    }

    public MongoCursor getMongoCursor(int type){
        MongoCollection<Document> collection = null;
        Bson filters = null;
        String indexType = null;
        if(type == 1){
            collection = mongo.mongoClient.getDatabase(mongo.getMongoData().getMongoDataBase()).getCollection(WEIXIN_TYPE);
            filters = Filters.and(Filters.gt("addTime", weixinLastTime), Filters.exists("pmid"));
            indexType = WEIXIN_INDEX;
            this.logConfInfo(WEIXIN_TYPE, indexType);
        }else if(type == 2){
            collection = mongo.mongoClient.getDatabase(mongo.getMongoData().getMongoDataBase()).getCollection(WEIBO_TYPE);
            filters = Filters.and(Filters.gt("addTime", weiboLastTime), Filters.exists("pmid"));
            indexType = WEIBO_INDEX;
            this.logConfInfo(WEIBO_TYPE, indexType);
        }
        FindIterable findIterable = collection.find(filters);
        mediaCount = collection.count(filters);
        return findIterable.iterator();
    }

    public List<Document> getDocMongo(MongoCursor cursor){
        if(!cursor.hasNext()){
            return null;
        }

        List<Document> docList = new ArrayList<>();
        int i = 0;
        while (cursor.hasNext()) {
            Document doc = (Document) cursor.next();
            long addTime = doc.getLong("addTime");
            if (addTime > lastTime) {
                lastTime = addTime;
            }
            docList.add(doc);
            i++;
            if (i >= esData.getBatchSize()){
                break;
            }
        }
        return docList;
    }


    public void insertToEs(int switchType, int mediaType, List<Document> conMapList){

        final String[] esType = new String[1];
        conMapList.forEach(m -> {

            XContentBuilder contMap = null;
            if (mediaType == 1){
                //contMap = EsMediaConvert.getWeixinMediaData(m);
                esType[0] = WEIXIN_INDEX;
            }else if (mediaType == 2){
                //contMap = EsMediaConvert.getWeiboMediaData(m);
                esType[0] = WEIBO_INDEX;
            }else if (mediaType == 2){
            }
            if(contMap == null){
                faildNum++;
                return;
            }
            if (switchType == 0){
                try {
                    esClient.upsert(esData.getIndexName(), esType[0], "pmid", m.getString("pmid"), contMap);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else {
                esClient.addMsg(contMap, esData.getIndexName(), esType[0], m.getString("pmid"));
            }
            if (esClient.getBulkNum() >= esData.getBatchSize()){
                synchronized (esLock){
                }
                esClient.excute();
            }
        });

    }

    @Override
    public void run() {
        List<Document> docList;
        while (true){
            synchronized (mongoLock){
                if (this.mongoCursor.hasNext()){
                    docList = this.getDocMongo(this.mongoCursor);
                }else {
                    break;
                }
            }
            this.insertToEs(this.switchType, this.mediaType, docList);
        }
    }
}
