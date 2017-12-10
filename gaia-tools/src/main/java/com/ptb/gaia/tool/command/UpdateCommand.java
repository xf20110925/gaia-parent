package com.ptb.gaia.tool.command;

import com.alibaba.fastjson.JSON;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.ptb.gaia.service.IGaia;
import com.ptb.gaia.service.IGaiaImpl;
import com.ptb.gaia.service.service.GWbMediaService;
import com.ptb.gaia.service.service.GWxMediaService;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by eric on 16/7/1.
 */
public class UpdateCommand {
    static Logger logger = LoggerFactory.getLogger(UpdateCommand.class);


    private static GWbMediaService gWbMediaService;
    private static GWxMediaService gWxMediaService;
    private static IGaia iGaia;
    private static MongoClient mongoClient;

    static {
        try {
            iGaia = new IGaiaImpl();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        try {
            gWbMediaService = new GWbMediaService();
            mongoClient = gWbMediaService.mongoClient;
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        try {
            gWxMediaService = new GWxMediaService();

        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

    }

    public static class BayouWxMediaAuthen {
        static class Authen {
            String bid;
            String username;
            String name;

            public Authen() {
            }

            public Authen(String bid, String username, String name) {
                this.bid = bid;
                this.username = username;
                this.name = name;
            }

            public String getBid() {
                return bid;
            }

            public void setBid(String bid) {
                this.bid = bid;
            }

            public String getUsername() {
                return username;
            }

            public void setUsername(String username) {
                this.username = username;
            }

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

            @Override
            public String toString() {
                return JSON.toJSONString(this);
            }
        }

        public static void syncBayouWxVerify(String path) {
            AtomicLong noVertifyCounter = new AtomicLong(0);
            try {
                Files.lines(Paths.get(path)).parallel().forEach(line -> {
                    try {
                        Authen authen = JSON.parseObject(line, Authen.class);
                        if (StringUtils.isBlank(authen.getName()) && StringUtils.isBlank(authen.getUsername())) {
                            System.out.println(String.format("未认证媒体个数->%s\t媒体：->%s", noVertifyCounter.addAndGet(1), authen));
                            return;
                        } else {
                            updateMongoMeida(authen);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        static MongoCollection<org.bson.Document> coll = mongoClient.getDatabase("gaia2").getCollection("wxMedia");

        static AtomicLong vertifyCounter = new AtomicLong(0);

        public static UpdateResult updateMongoMeida(Authen authen) {
            UpdateResult updateResult = null;
            try {
                UpdateOptions upsert = new UpdateOptions().upsert(false);
                Bson bson = new org.bson.Document("$set", new org.bson.Document("authInfo", authen.getName()).append("isAuth", StringUtils.isNotBlank(authen.getName()) ? 1 : 0));
                updateResult = coll.updateOne(Filters.eq("_id", authen.getBid()), bson, upsert);
                System.out.println(String.format("已更新认证媒体个数->%s\t媒体：->%s", vertifyCounter.addAndGet(1), authen));
            } catch (Exception e) {
                e.printStackTrace();
            }
            return updateResult;
        }

        public static void main(String[] args) {
            syncBayouWxVerify("g:/biz_verify_final.json");
        }
    }

}
