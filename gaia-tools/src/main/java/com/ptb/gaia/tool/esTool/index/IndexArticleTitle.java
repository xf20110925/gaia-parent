package com.ptb.gaia.tool.esTool.index;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.ptb.gaia.tool.esTool.convert.EsArticleConvert;
import com.ptb.gaia.tool.esTool.data.EsData;
import com.ptb.gaia.tool.esTool.data.MongoData;
import com.ptb.gaia.tool.esTool.util.EsTransportClient;
import com.ptb.gaia.tool.esTool.util.MongoClientm;
import com.ptb.gaia.tool.utils.FileUtils;
import com.ptb.utils.exception.PTBException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.seleniumhq.jetty7.util.security.Credential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by watson zhang on 2017/1/10.
 */
public class IndexArticleTitle {
    private static Logger logger = LoggerFactory.getLogger(EsArticleConvert.class);
    private static Configuration conf;

    private static String WEIXIN_COLL;
    private static String WEIBO_COLL;

    private static String WEIXIN_INDEX = "wxArticle";
    private static String WEIBO_INDEX = "wbArticle";

    private static int intervalHour;
    private long mediaCount;
    long lastTime;

    public MongoData mongoData;
    public MongoClientm mongo;
    public EsTransportClient esClient;
    public EsData esData;
    public static FileUtils fileUtils;

    public IndexArticleTitle(){
        try {
            conf = new PropertiesConfiguration("ptb.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
            throw new PTBException("GAIA配置文件不正常", e);
        }
        intervalHour = conf.getInt("gaia.tools.article.intervalHour", 1);
        WEIXIN_COLL = conf.getString("gaia.tools.article.mongo.wxcoll", "wxArticle");
        WEIBO_COLL = conf.getString("gaia.tools.article.mongo.wbcoll", "wbArticle");

        mongo = new MongoClientm();
        mongoData = mongo.getMongoData();

        esClient = new EsTransportClient();
        esData = esClient.getEsData();
        esData.setIndexName(conf.getString("gaia.es.article.index", "article_m"));
        fileUtils = new FileUtils();
    }


    protected static XContentBuilder convertDocToTitleObject(Document doc, int type){
        double relRate = 1.0;

        int readIntNum = 0;
        if (type == 1){
            if (doc.getInteger("readNum") != null){
                readIntNum = Integer.valueOf(doc.getInteger("readNum"));
            }
        }else if (type == 2){
            if (doc.getInteger("zpzNum") != null){
                readIntNum = Integer.valueOf(doc.getInteger("zpzNum"));
            }
        }
        relRate += readIntNum/100001.0;

        //likeNum 权重设置
        int likeIntNum = 0;
        if (doc.getInteger("likeNum") != null){
            likeIntNum = Integer.valueOf(doc.getInteger("likeNum"));
        }
        relRate += likeIntNum/10000.0;

        double origScore = 0.5;
        if (doc.getInteger("isOriginal") != null && doc.getInteger("isOriginal").equals(1)){
            relRate += origScore;
        }else {
            relRate -= origScore;
        }
        XContentBuilder contentBuilderTitle = null;
        try {
            contentBuilderTitle = JsonXContent.contentBuilder().startObject();
            if (doc.getInteger("readNum") != null){
                contentBuilderTitle.field("readNum", doc.getInteger("readNum"));
            }

            if (doc.getInteger("zpzNum") != null){
                contentBuilderTitle.field("zpzNum", doc.getInteger("zpzNum"));
            }

            if (doc.getInteger("isOriginal") != null){
                contentBuilderTitle.field("isOriginal", doc.getInteger("isOriginal"));
            }

            String title = doc.getString("title");
            if (title != null){
                contentBuilderTitle.field("title", title);
            }else {
                //logger.info("there are no title! {}", doc.getString("articleUrl"));
                return null;
            }

            if (doc.getString("pmid") != null){
                contentBuilderTitle.field("pmid", doc.getString("pmid"));
            }

            if (doc.getLong("postTime") != null){
                contentBuilderTitle.field("postTime", doc.getLong("postTime"));
            }

            contentBuilderTitle.field("preScore", relRate);

            if (doc.getString("articleUrl") != null){
                contentBuilderTitle.field("articleUrl", doc.getString("articleUrl"));
            }

            contentBuilderTitle.endObject();
        } catch (IOException e) {
            //e.printStackTrace();
            logger.error("to title error!", e);
        }
        return contentBuilderTitle;
    }

    public void logConfInfo(String indexType){
        logger.info(" mongo host:\t{}\n port      :\t{}\n db        :\t{}\n wxcoll      :\t{}\n wbcoll       :\t{}",
                mongoData.getMongoHostList(),
                mongoData.getMongoPort(),
                mongoData.getMongoDataBase(),
                WEIXIN_COLL,
                WEIBO_COLL);
        logger.info(" es hosts :\t{}\n cluster  :\t{}\n index:\t{}\n type:\t{}\n batchSize:\t{}\n intervalHour:\t{}",
                esClient.getEsData().getHostNames(),
                esClient.getEsData().getClusterName(),
                esData.getIndexName(),
                indexType,
                esData.getBatchSize(),
                intervalHour);
    }

    public MongoCursor getMongoColl(int type){
        MongoClientm mongo = new MongoClientm();
        MongoCollection<Document> collection = null;
        Bson filters = null;
        if(type == 1){
            collection = mongo.mongoClient.getDatabase(mongo.getMongoData().getMongoDataBase()).getCollection(WEIXIN_COLL);
            filters = Filters.and(Filters.gt("updateTime", lastTime));
        }else if(type == 2){
            collection = mongo.mongoClient.getDatabase(mongo.getMongoData().getMongoDataBase()).getCollection(WEIBO_COLL);
            filters = Filters.and(Filters.gt("updateTime", lastTime));
        }
        FindIterable findIterable = collection.find(filters);
        mediaCount = collection.count(filters);
        return findIterable.iterator();
    }


    public void importToEs(int type){
        String esArticleType = null;
        if (type == 1){
            esArticleType = WEIXIN_INDEX;
        }else if (type == 2){
            esArticleType = WEIBO_INDEX;
        }
        this.logConfInfo(esArticleType);

        int filedNum = 0;
        int i = 0;
        long currentNum = 0;
        int timeMinute = 0;
        long internalTime = 60000;
        long startTime = System.currentTimeMillis();
        MongoCursor mongoCursor = this.getMongoColl(type);
        logger.info("get cursor success, total num: {}", mediaCount);

        logger.info("start task...");
        long lastTime = 0;

        while (mongoCursor.hasNext()){
            try {
                Document doc = (Document) mongoCursor.next();
                if (doc.getLong("updateTime") != null && doc.getLong("updateTime") > lastTime){
                    lastTime = doc.getLong("updateTime");
                }
                XContentBuilder contentBuilder = null;
                contentBuilder = this.convertDocToTitleObject(doc, type);
                if (contentBuilder == null){
                    filedNum++;
                    continue;
                }
                String urlMd5 = Credential.MD5.digest(doc.getString("_id")).substring(4);
                esClient.addMsg(contentBuilder, esData.getIndexName(), esArticleType, urlMd5);
                i++;
                if (i >= esData.getBatchSize()){
                    esClient.excute();
                    long endTime = System.currentTimeMillis();
                    currentNum += i;
                    i = 0;
                    if ((endTime - startTime)/internalTime  > timeMinute){
                        timeMinute++;
                        logger.info("current num: {}, total num: {}, minutes: {}, notitle: {}", currentNum, mediaCount, timeMinute, filedNum);
                    }
                }
            }catch (Exception e){
                //e.printStackTrace();
                logger.error("convert error!", e);
            }
        }
        if(i != 0){
            esClient.excute();
            currentNum += i;
        }

        logger.info("successful end...");
        logger.info("end num: {}, filedNum: {}, total num: {}", currentNum, filedNum, mediaCount);
    }

    public static void articleContentEntry(int articleType, long lastTime){
        if (articleType != 1 && articleType != 2){
            logger.error("args is 1 or 2");
            return;
        }
        IndexArticleContent articleConvert = new IndexArticleContent();
        articleConvert.lastTime = lastTime;
        articleConvert.importToEs(articleType);
    }

}
