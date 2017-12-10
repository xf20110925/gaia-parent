package com.ptb.gaia.search.utils;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by watson zhang on 16/6/30.
 */
public class MongoUtil {

    //public MongoCollection dc;
    private static Logger logger = LoggerFactory.getLogger(MongoUtil.class);
    UpdateOptions up = new UpdateOptions().upsert(true);
    public MongoClient mongoClient;
    public MongoDatabase mongoDatabase;
    private PropertiesConfiguration conf;
    public MongoData mongoData = new MongoData();

    public MongoUtil(){
        try {
            conf = new PropertiesConfiguration("ptb.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        mongoData.setMongoHost(conf.getString("gaia.tools.mongo.host"));
        mongoData.setMongoPort(conf.getInt("gaia.tools.mongo.port"));
        mongoData.setMongoDataBase(conf.getString("gaia.tools.mongo.dbName"));
        //mongoData.setMongoColl(conf.getString("athene.mongo.coll"));

        if(mongoData == null){
            logger.error("mongo data is null");
            return;
        }
        this.init();
    }

    public MongoData getMongoData() {
        return mongoData;
    }

    private void init(){
        mongoClient = new MongoClient(mongoData.mongoHost, mongoData.mongoPort);
        mongoDatabase = mongoClient.getDatabase(mongoData.getMongoDataBase());
    }

    public void restart(){
        if(mongoClient != null){
            mongoClient.close();
            mongoClient = null;
        }
        mongoClient = new MongoClient(mongoData.mongoHost, mongoData.mongoPort);
        mongoDatabase = mongoClient.getDatabase(mongoData.getMongoDataBase());
    }

}
