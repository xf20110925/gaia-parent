package com.ptb.gaia.tool.esTool.convert;

import com.alibaba.fastjson.JSON;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.ptb.gaia.index.EsArticleIndexImp;
import com.ptb.gaia.index.bean.EsInsertData;
import com.ptb.gaia.search.utils.EsUtils;
import com.ptb.gaia.tool.esTool.data.MongoData;
import com.ptb.gaia.tool.esTool.util.MongoClientm;
import com.ptb.utils.exception.PTBException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.seleniumhq.jetty7.util.security.Credential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.ptb.gaia.search.article.IArticleSearch.TWbArticleType;
import static com.ptb.gaia.search.article.IArticleSearch.TWxArticleType;

/**
 * Created by eric on 16/8/9.
 */
public class EsArticleConvert {
    private static Logger logger = LoggerFactory.getLogger(EsArticleConvert.class);
    static TransportClient cli = EsUtils.getCli();
    private String articleIndexType;
    private Configuration conf;

    private String WEIXIN_COLL;
    private String WEIBO_COLL;

    private String WEIXIN_INDEX = "wxArticle";
    private String WEIBO_INDEX = "wbArticle";

    private long lastTime;
    private long weixinLastTime;
    private long weiboLastTime;
    private int intervalHour;
    private long mediaCount;
    private static int ARTICLE_TITLE = 1;
    private static int ARTICLE_CONTENT = 2;

    int batchSize;
    String[] hostArray;
    String clusterName;
    String titleIndex;
    String contentIndex;

    public MongoData mongoData;
    public MongoClientm mongo;
    public EsArticleIndexImp esArticleIndexImp;

    public EsArticleConvert(){
        try {
            conf = new PropertiesConfiguration("ptb.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
            throw new PTBException("GAIA配置文件不正常", e);
        }
        WEIXIN_COLL = conf.getString("gaia.tools.article.mongo.wxcoll", "wxArticle");
        WEIBO_COLL = conf.getString("gaia.tools.article.mongo.wbcoll", "wbArticle");
        hostArray = conf.getStringArray("gaia.es.hosts");
        clusterName = conf.getString("gaia.es.clusterName", "ptbes");
        titleIndex = conf.getString("gaia.es.article.title.index", "article_m");
        contentIndex = conf.getString("gaia.es.article.content.index", "article_m");
        lastTime = conf.getLong("gaia.convert.lastTime", 0);
        weixinLastTime = lastTime;
        weiboLastTime = lastTime;
        intervalHour = conf.getInt("gaia.convert.intervalHour", 24);
        batchSize = conf.getInt("gaia.es.batchSize", 3000);
        articleIndexType = conf.getString("gaia.es.article.index", "article");
        mongo = new MongoClientm();
        mongoData = mongo.getMongoData();

        esArticleIndexImp = new EsArticleIndexImp();
    }

    public void createAllIndex() throws IOException {
        cli.prepareIndex(articleIndexType, TWxArticleType);
        XContentBuilder xContentBuilder = null;
        try {
            xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("analyzer").
                    startObject("analyzer").
                    startObject("ik").field("tokenizer", "ik").
                    endObject().
                    endObject().
                    endObject().field("index.refresh_interval", "10s").endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        CreateIndexResponse createIndexResponse = cli.admin().indices().prepareCreate(articleIndexType).setSettings(xContentBuilder).execute().actionGet();
        System.out.println(createIndexResponse.isAcknowledged());
        ClusterHealthResponse clusterHealthResponse = cli.admin().cluster().health(new ClusterHealthRequest(articleIndexType).waitForYellowStatus()).actionGet();

        System.out.println(String.format(String.format("创建 index [%s] ---------------------------- 成功", articleIndexType)));

    }

    public void createArticleMapping() throws IOException {
        cli.admin().indices().putMapping(createMapping(articleIndexType, TWxArticleType)).actionGet();
        System.out.println(String.format(String.format("创建 mapping [%s] ---------------------------- 成功", TWxArticleType)));
        cli.admin().indices().putMapping(createMapping(articleIndexType, TWbArticleType)).actionGet();
        System.out.println(String.format(String.format("创建 mapping [%s] ---------------------------- 成功", TWbArticleType)));
    }

    private PutMappingRequest createMapping(String index, String type) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject(type).startObject("_all").field("analyzer", "ik").field("search_analyzer", "ik").endObject().startObject("properties").startObject("content").field("type", "string").field("term_vector", "with_positions_offsets").field("analyzer", "ik").field("search_analyzer", "ik").field("boost", 5).endObject().startObject("title").field("type", "string").field("term_vector", "with_positions_offsets").field("analyzer", "ik").field("search_analyzer", "ik").field("boost", 3).endObject().endObject().endObject();
        return Requests.putMappingRequest(index).type(type).source(builder);
    }

    public void logConfInfo(String indexType){
        logger.error(" mongo host:\t{}\n port      :\t{}\n db        :\t{}\n wxcoll      :\t{}\n wbcoll       :\t{}\nwbLastTime:\t{}\nwxLastTime:{}",
                mongoData.getMongoHostList(),
                mongoData.getMongoPort(),
                mongoData.getMongoDataBase(),
                WEIXIN_COLL,
                WEIBO_COLL,
                weiboLastTime,
                weixinLastTime);
        logger.error(" es hosts :\t{}\n cluster  :\t{}\n titleIndex:\t{}\ncontentIndex:\t{}\n type:\t{}\n batchSize:\t{}\n intervalHour:\t{}",
                hostArray,
                clusterName,
                titleIndex,
                contentIndex,
                indexType,
                batchSize,
                intervalHour);
    }

    public MongoCursor getMongoColl(int type){
        MongoClientm mongo = new MongoClientm();
        MongoCollection<Document> collection = null;
        Bson filters = null;
        if(type == 1){
            collection = mongo.mongoClient.getDatabase(mongo.getMongoData().getMongoDataBase()).getCollection(WEIXIN_COLL);
            filters = Filters.and(Filters.gt("updateTime", weixinLastTime));
        }else if(type == 2){
            collection = mongo.mongoClient.getDatabase(mongo.getMongoData().getMongoDataBase()).getCollection(WEIBO_COLL);
            filters = Filters.and(Filters.gt("updateTime", weiboLastTime));
        }
        FindIterable findIterable = collection.find(filters);
        if (findIterable == null){
            return null;
        }
        mediaCount = collection.count(filters);
        return findIterable.iterator();
    }

    private void mongoImportToEs(int type){
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
        logger.error("get cursor success, total num: {}", mediaCount);

        logger.error("start task...");

        while (mongoCursor.hasNext()){
            try {
                Document doc = (Document) mongoCursor.next();
                if (doc.getLong("updateTime") != null && doc.getLong("updateTime") > lastTime){
                    lastTime = doc.getLong("updateTime");
                }

                EsInsertData esInsertData = new EsInsertData();
                String urlMd5 = Credential.MD5.digest(doc.getString("_id")).substring(4);
                esInsertData.setId(urlMd5);
                esInsertData.setJsonObject(JSON.parseObject(doc.toJson()));
                if (type == 1){
                    esArticleIndexImp.insertWxArticleTitleFromMongo(esInsertData);
                    esArticleIndexImp.insertWxArticleContentFromMongo(esInsertData);
                }else if (type == 2){
                    esArticleIndexImp.insertWbArticleTitleFromMongo(esInsertData);
                    esArticleIndexImp.insertWbArticleContentFromMongo(esInsertData);
                }

                i++;
                if (i >= batchSize){
                    long endTime = System.currentTimeMillis();
                    currentNum += i;
                    i = 0;
                    if ((endTime - startTime)/internalTime  > timeMinute){
                        timeMinute++;
                        logger.error("current num: {}, total num: {}, minutes: {}, notitle: {}, lastTime: {}", currentNum, mediaCount, timeMinute, filedNum, lastTime);
                    }
                }
            }catch (Exception e){
                //e.printStackTrace();
                logger.error("convert error!", e);
            }
        }
        esArticleIndexImp.flushEsBulk();
        if(i != 0){
            currentNum += i;
        }
        if (lastTime != 0){
            if(type == 1){
                weixinLastTime = lastTime;
            }else if(type == 2){
                weiboLastTime = lastTime;
            }
        }

        logger.error("successful end...");
        logger.error("end num: {}, filedNum: {}, total num: {}", currentNum, filedNum, mediaCount);
        logger.error("wbLastTime: {}, wxLastTime: {}", weiboLastTime, weixinLastTime);
    }

    public static void articleEntry(int mediaType, long lastTime){
        if (mediaType != 1 && mediaType != 2){
            logger.error("media type is 1(weixin) or 2(weibo)");
            return;
        }

        EsArticleConvert esArticleConvert = new EsArticleConvert();
        if (mediaType == 1){
            esArticleConvert.weixinLastTime = lastTime;
        }else if (mediaType == 2){
            esArticleConvert.weiboLastTime = lastTime;
        }
        esArticleConvert.mongoImportToEs(mediaType);
    }

    public static void ArticleIncrementEntry(){

        EsArticleConvert esArticleConvert = new EsArticleConvert();
        esArticleConvert.weiboLastTime = new Date().getTime();
        esArticleConvert.weixinLastTime = new Date().getTime();
        while (true){
            esArticleConvert.mongoImportToEs(1);
            esArticleConvert.mongoImportToEs(2);

            logger.info("import success {}", new Date());
            try {
                Thread.sleep(TimeUnit.HOURS.toMillis(esArticleConvert.intervalHour));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        EsArticleConvert esArticleConvert = new EsArticleConvert();
        esArticleConvert.mongoImportToEs(1);
    }
}
