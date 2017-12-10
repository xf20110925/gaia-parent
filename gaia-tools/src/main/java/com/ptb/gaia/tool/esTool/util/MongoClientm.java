package com.ptb.gaia.tool.esTool.util;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import com.ptb.gaia.tool.esTool.data.MongoData;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by watson zhang on 16/6/30.
 */
public class MongoClientm {

    //public MongoCollection dc;
    private static Logger logger = LoggerFactory.getLogger(MongoClientm.class);
    UpdateOptions up = new UpdateOptions().upsert(true);
    public MongoClient mongoClient;
    private PropertiesConfiguration conf;
    public MongoData mongoData = new MongoData();

    public MongoClientm(){
        try {
            conf = new PropertiesConfiguration("ptb.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        mongoData.setMongoHostList(conf.getList("gaia.tools.mongo.host"));
        mongoData.setMongoPort(conf.getInt("gaia.tools.mongo.port"));
        mongoData.setMongoDataBase(conf.getString("gaia.tools.mongo.dbName", "gaia3"));

        if(mongoData == null){
            logger.error("mongo data is null");
            return;
        }
        this.initMongo(mongoData);
    }

    public MongoClientm(MongoData mongoData){
        if (mongoData == null){
            logger.error("mongo data is null");
            return;
        }
        this.mongoData = mongoData;

        this.init();
    }

    public MongoData getMongoData() {
        return mongoData;
    }

    private void initMongo(MongoData mongoData){
        if (mongoData == null){
            return;
        }
        List<ServerAddress> addresses = new ArrayList<>();
        mongoData.getMongoHostList().forEach(h -> {
            ServerAddress address = new ServerAddress(h.toString(), mongoData.getMongoPort());
            addresses.add(address);
        });
        ReadPreference readPreference = ReadPreference.secondaryPreferred();
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.readPreference(readPreference);
        mongoClient = new MongoClient(addresses, builder.build());

        return;
    }

    private void init(){
        mongoClient = new MongoClient(mongoData.mongoHost, mongoData.mongoPort);
    }

    public void restart(MongoData mongoData){
        if(mongoClient != null){
            mongoClient.close();
            mongoClient = null;
        }
        this.initMongo(mongoData);
    }

    public static void main(String[] args){
        MongoClient mongoClient;
        List<ServerAddress> addresses = new ArrayList<ServerAddress>();
        ServerAddress address1 = new ServerAddress("192.168.40.18" , 27017);
        ServerAddress address2 = new ServerAddress("192.168.40.19" , 27017);
        ServerAddress address3 = new ServerAddress("192.168.40.20" , 27017);
        addresses.add(address1);
        addresses.add(address2);
        addresses.add(address3);
        ReadPreference readPreference = ReadPreference.secondaryPreferred();
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.readPreference(readPreference);
        mongoClient = new MongoClient(addresses, builder.build());
        FindIterable<Document> documents = mongoClient.getDatabase("gaia2").getCollection("wbMedia").find();
        MongoCursor cursor = documents.iterator();
        while (cursor.hasNext()){
            System.out.println(cursor.next());
        }
    }
}
