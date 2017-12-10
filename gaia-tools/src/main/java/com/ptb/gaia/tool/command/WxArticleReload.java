package com.ptb.gaia.tool.command;

import com.alibaba.fastjson.JSON;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.result.DeleteResult;
import com.ptb.gaia.bus.kafka.KafkaBus;
import com.ptb.gaia.etl.flume.utils.GaiaConvertUtils;
import com.ptb.gaia.service.IGaia;
import com.ptb.gaia.service.IGaiaImpl;
import com.ptb.gaia.service.entity.article.GWxArticleBasic;
import com.ptb.uranus.common.entity.CollectType;
import com.ptb.uranus.schedule.model.Priority;
import com.ptb.uranus.schedule.model.SchedulableCollectCondition;
import com.ptb.uranus.schedule.model.ScheduleObject;
import com.ptb.uranus.schedule.trigger.JustOneTrigger;
import com.ptb.uranus.server.send.BusSender;
import com.ptb.uranus.server.send.Sender;
import com.ptb.uranus.server.send.entity.article.WeixinArticleStatic;
import com.ptb.uranus.server.send.entity.convert.SendObjectConvertUtil;
import com.ptb.uranus.spider.weixin.WeixinSpider;
import com.ptb.uranus.spider.weixin.bean.WxArticle;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @DESC:
 * @VERSION:
 * @author: xuefeng
 * @Date: 2016/11/3
 * @Time: 13:29
 */
public class WxArticleReload {
    public enum MongoUtil {
        INSTANCESHARD(getMongoClientV1("172.16.0.216", 27017)), INSTACENEWMONGO(getMongoClientV2("172.16.0.54|172.16.0.55|172.16.0.56", 27017));
        //			INSTANCESHARD(getMongoClientV1("192.168.40.18", 27017)), INSTACENEWMONGO(getMongoClientV2("192.168.40.18|192.168.40.19|192.168.40.20", 27017));
        private MongoClient mongoClient;

        MongoUtil(MongoClient mongoClient) {
            this.mongoClient = mongoClient;
        }

        private static MongoClient getMongoClientV1(String host, int port) {
            MongoClientOptions.Builder options = new MongoClientOptions.Builder();
            options.connectionsPerHost(300);
            options.connectTimeout(30000);
            options.maxWaitTime(5000);
            options.socketTimeout(0);
            options.writeConcern(WriteConcern.SAFE);
            return new MongoClient(new ServerAddress(host, port), options.build());
        }

        private static MongoClient getMongoClientV2(String host, int port) {
            ReadPreference readPreference = ReadPreference.secondaryPreferred();
            List<ServerAddress> addresses = Arrays.asList(host.split("\\|")).stream().map(x -> new ServerAddress(x, port)).collect(Collectors.toList());
            MongoClientOptions.Builder options = new MongoClientOptions.Builder().readPreference(readPreference).connectionsPerHost(300).connectTimeout(30000).maxWaitTime(5000).socketTimeout(0).threadsAllowedToBlockForConnectionMultiplier(5000).writeConcern(WriteConcern.ACKNOWLEDGED);
            return new MongoClient(addresses, options.build());
        }


        public MongoDatabase getDatabase(String dbName) {
            if (StringUtils.isNotBlank(dbName)) {
                MongoDatabase database = this.mongoClient.getDatabase(dbName);
                return database;
            }
            throw new IllegalArgumentException("name can not be null!");
        }

        public MongoCollection<Document> getCollection(String dbName, String collName) {
            return getDatabase(dbName).getCollection(collName);
        }

        public MongoClient getMongoClient() {
            return mongoClient;
        }

        public void setMongoClient(MongoClient mongoClient) {
            this.mongoClient = mongoClient;
        }
    }


    public void handleArticles() throws IOException {
        List<String> urls = Files.lines(Paths.get("./run.log")).collect(Collectors.toList());
        FindIterable<Document> docs = MongoUtil.INSTANCESHARD.getDatabase("gaia2").getCollection("wxArticle").find(Filters.in("_id", urls));
        LinkedList<Document> docList = new LinkedList<>();
        for (Document doc : docs) {
            docList.add(doc);
        }
        System.out.println(String.format("从分片mongo查询微信文章%s篇", docList.size()));
        MongoUtil.INSTACENEWMONGO.getDatabase("gaia2").getCollection("wxArticle").insertMany(docList, new InsertManyOptions().ordered(false));
        System.out.println("插入成功");
    }

    public void handleMedias() throws IOException {
        List<String> pmids = Files.lines(Paths.get("./run.log")).collect(Collectors.toList());
        FindIterable<Document> docs = MongoUtil.INSTANCESHARD.getDatabase("gaia2").getCollection("wxMedia").find(Filters.in("_id", pmids));
        LinkedList<Document> docList = new LinkedList<>();
        for (Document doc : docs) {
            docList.add(doc);
        }
        System.out.println(String.format("从分片mongo查询微信媒体%s个", docList.size()));
        MongoUtil.INSTACENEWMONGO.getDatabase("gaia2").getCollection("wxMedia").insertMany(docList, new InsertManyOptions().ordered(false));
        System.out.println("插入成功");
    }

    public void wxArticleStaticsAdd(String fileName) throws IOException, ConfigurationException {
        List<String> urls = Files.lines(Paths.get(fileName)).collect(Collectors.toList());
        KafkaBus bus = new KafkaBus();
        Sender sender = new BusSender(bus);
        bus.start(false, 5);
        WeixinSpider wxSpider = new WeixinSpider();
        IGaia iGaia = new IGaiaImpl();
        urls.stream().forEach(url -> {
            Optional<WxArticle> wxArticleOpt = wxSpider.getArticleByUrl(url);
            wxArticleOpt.ifPresent(wxArticle -> {
                wxArticle.setArticleUrl(url);
                WeixinArticleStatic wxArticleStatic = SendObjectConvertUtil.weixinArticleStaticConvert(wxArticle);
                GWxArticleBasic gWxArticleBasic = GaiaConvertUtils.convertJSONObjectToGWxArticleBasic(JSON.parseObject(JSON.toJSONString(wxArticleStatic)));
                iGaia.addWxArticleBasicBatch(Arrays.asList(gWxArticleBasic));
//                System.out.println("发送微信文章" + JSON.toJSONString(wxArticleStatic));
//                sender.sendArticleStatic(wxArticleStatic);
            });
        });
    }

    public void handleWbArticles() throws IOException {
        List<String> urls = Files.lines(Paths.get("./run.log")).collect(Collectors.toList());
        FindIterable<Document> docs = MongoUtil.INSTANCESHARD.getDatabase("gaia2").getCollection("wbArticleTest1").find(Filters.in("_id", urls));
        LinkedList<Document> docList = new LinkedList<>();
        for (Document doc : docs) {
            docList.add(doc);
        }
        System.out.println(String.format("从分片mongo查询微博文章%s篇", docList.size()));
        MongoUtil.INSTACENEWMONGO.getDatabase("gaia2").getCollection("wbArticle").insertMany(docList, new InsertManyOptions().ordered(false));
        System.out.println("插入成功");
    }

    //cleanType -> C_WB_A_N, C_WX_A_N
    public void scheduleClean(String cleanType, String mediaDB, String mediaColl) {
        FindIterable<Document> scheduleDocs = MongoUtil.INSTACENEWMONGO.getDatabase("uranus").getCollection("schedule").find(Filters.eq("obj.collectType", cleanType));
        Map<String, String> idConditionMap = new HashMap<>();
        for (Document doc : scheduleDocs) {
            idConditionMap.put(doc.getString("_id"), ((Document) doc.get("obj")).getString("conditon").split(":::")[0]);
        }
        FindIterable<Document> wbMDocs = MongoUtil.INSTACENEWMONGO.getDatabase(mediaDB).getCollection(mediaColl).find().projection(new Document("_id", 1));
        Set<String> pmids = new HashSet<>();
        for (Document doc : wbMDocs) {
            pmids.add(doc.getString("_id"));
        }
        List<String> delSchedule = new LinkedList<>();
        idConditionMap.forEach((k, v) -> {
            if (!pmids.contains(v)) delSchedule.add(k);
        });
        System.out.println(String.format("清理%s个数%d,_ids->%s", cleanType, delSchedule.size(), delSchedule));
        DeleteResult ret = MongoUtil.INSTACENEWMONGO.getDatabase("uranus").getCollection("schedule").deleteMany(Filters.in("_id", delSchedule));
        System.out.println(String.format("清理完成，清理个数为%d", ret.getDeletedCount()));
    }

    //微信文章表动态数据加入调度重新爬取
    public void wxA2Schedule(String url) {
        AtomicLong counter = new AtomicLong(0);
        ScheduleObject<SchedulableCollectCondition> scheduleObject = new ScheduleObject<>(new JustOneTrigger(), Priority.L1, new SchedulableCollectCondition(CollectType.C_WX_A_D, url));
        Document doc = Document.parse(scheduleObject.toString());
        Object id = doc.put("_id", scheduleObject.getId());
        doc.remove(id);
        MongoUtil.INSTACENEWMONGO.getDatabase("uranus").getCollection("schedule").insertOne(doc);
        System.out.println(String.format("插入-》%d条调度任务", counter.addAndGet(1)));
    }

    public static void main(String[] args) throws IOException {
    }

}
