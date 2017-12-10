//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.ptb.gaia.service.utils;

import com.mongodb.*;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MongoUtils1 {

    private static MongoClient mongoClient;
    private static Logger logger = LoggerFactory.getLogger(MongoUtils1.class);
    private static String mongoHost;
    private static int mongoPort;
    private static String lock = "xxxxx";

    static public void configure(String host, int port) {
        mongoHost = host;
        mongoPort = port;
    }

    static public MongoClient instance() {
        if (mongoClient == null) {
            synchronized (lock) {
                if (mongoClient == null) {
                    logger.info("正在配置MONGODB");
                    Builder options = new Builder();
                    options.connectionsPerHost(300);
                    options.connectTimeout(30000);
                    options.maxWaitTime(5000);
                    options.socketTimeout(0);
                    options.threadsAllowedToBlockForConnectionMultiplier(5000);
                    options.writeConcern(WriteConcern.ACKNOWLEDGED);
                    try {
                        mongoClient = new MongoClient(new ServerAddress(mongoHost, mongoPort), options.build());
                    } catch (MongoException e) {
                        throw new RuntimeException(String.format("connect mongo [%s:%d] failed", mongoHost, mongoPort));
                    }
                    logger.info("MONGO配置完成");
                }
            }
        }
        return mongoClient;
    }


    public MongoDatabase getDatabase(String dbName) {
        if (dbName != null && !"".equals(dbName)) {
            MongoDatabase database = this.mongoClient.getDatabase(dbName);
            return database;
        } else {
            return null;
        }
    }

    public DB getDB(String dbName) {
        return dbName != null && !"".equals(dbName) ? this.mongoClient.getDB(dbName) : null;
    }

    public MongoCollection<Document> getCollection(String dbName, String collName) {
        if (null != collName && !"".equals(collName)) {
            if (null != dbName && !"".equals(dbName)) {
                MongoCollection collection = this.mongoClient.getDatabase(dbName).getCollection(collName);
                return collection;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    public List<String> getAllCollections(String dbName) {
        MongoIterable colls = this.getDatabase(dbName).listCollectionNames();
        ArrayList _list = new ArrayList();
        MongoCursor var4 = colls.iterator();

        while (var4.hasNext()) {
            String s = (String) var4.next();
            _list.add(s);
        }

        return _list;
    }

    public MongoIterable<String> getAllDBNames() {
        MongoIterable s = this.mongoClient.listDatabaseNames();
        return s;
    }

    public void dropDB(String dbName) {
        this.getDatabase(dbName).drop();
    }

    public Document findById(MongoCollection<Document> coll, String id) {
        ObjectId _idobj = null;

        try {
            _idobj = new ObjectId(id);
        } catch (Exception var5) {
            return null;
        }

        Document myDoc = (Document) coll.find(Filters.eq("_id", _idobj)).first();
        return myDoc;
    }

    public int getCount(MongoCollection<Document> coll) {
        int count = (int) coll.count();
        return count;
    }

    public MongoCursor<Document> find(MongoCollection<Document> coll, Bson filter) {
        return coll.find(filter).iterator();
    }

    public MongoCursor<Document> findByPage(MongoCollection<Document> coll, Bson filter, int pageNo, int pageSize) {
        BasicDBObject orderBy = new BasicDBObject("_id", Integer.valueOf(1));
        return coll.find(filter).sort(orderBy).skip((pageNo - 1) * pageSize).limit(pageSize).iterator();
    }

    public int deleteById(MongoCollection<Document> coll, String id) {
        boolean count = false;
        ObjectId _id = null;

        try {
            _id = new ObjectId(id);
        } catch (Exception var7) {
            return 0;
        }

        Bson filter = Filters.eq("_id", _id);
        DeleteResult deleteResult = coll.deleteOne(filter);
        int count1 = (int) deleteResult.getDeletedCount();
        return count1;
    }

    public Document updateById(MongoCollection<Document> coll, String id, Document newdoc) {
        ObjectId _idobj = null;

        try {
            _idobj = new ObjectId(id);
        } catch (Exception var6) {
            return null;
        }

        Bson filter = Filters.eq("_id", _idobj);
        coll.updateOne(filter, new Document("$set", newdoc));
        return newdoc;
    }

    public Document pushById(MongoCollection<Document> coll, String id, Document newdoc) {
        ObjectId _idobj = null;

        try {
            _idobj = new ObjectId(id);
        } catch (Exception var6) {
            return null;
        }

        Bson filter = Filters.eq("_id", _idobj);
        coll.updateOne(filter, new Document("$push", newdoc));
        return newdoc;
    }

    public void dropCollection(String dbName, String collName) {
        this.getDB(dbName).getCollection(collName).drop();
    }

    public void close() {
        if (this.mongoClient != null) {
            this.mongoClient.close();
            this.mongoClient = null;
        }
    }

    public static void main(String[] args) {
        String s = new String();

    }
}
