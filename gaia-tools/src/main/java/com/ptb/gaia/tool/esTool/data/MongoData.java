package com.ptb.gaia.tool.esTool.data;

import java.util.List;

/**
 * Created by watson zhang on 16/6/30.
 */
public class MongoData {
    public String mongoHost;
    public List<Object> mongoHostList;
    public int mongoPort;
    public String mongoDataBase;
    public String mongoColl;

    public String getMongoHost() {
        return mongoHost;
    }

    public void setMongoHost(String mongoHost) {
        this.mongoHost = mongoHost;
    }

    public List<Object> getMongoHostList() {
        return mongoHostList;
    }

    public void setMongoHostList(List<Object> mongoHostList) {
        this.mongoHostList = mongoHostList;
    }

    public int getMongoPort() {
        return mongoPort;
    }

    public void setMongoPort(int mongoPort) {
        this.mongoPort = mongoPort;
    }

    public String getMongoDataBase() {
        return mongoDataBase;
    }

    public void setMongoDataBase(String mongoDataBase) {
        this.mongoDataBase = mongoDataBase;
    }

    public String getMongoColl() {
        return mongoColl;
    }

    public void setMongoColl(String mongoColl) {
        this.mongoColl = mongoColl;
    }
}
