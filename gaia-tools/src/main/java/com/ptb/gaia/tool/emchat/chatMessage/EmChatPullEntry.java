package com.ptb.gaia.tool.emchat.chatMessage;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.mongodb.DBCursor;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.ptb.gaia.emchat.api.ChatMessageAPI;
import com.ptb.gaia.emchat.comm.ClientContext;
import com.ptb.gaia.emchat.comm.EasemobRestAPIFactory;
import com.ptb.gaia.emchat.comm.wrapper.ResponseWrapper;
import com.ptb.gaia.tool.utils.MongoUtil;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by watson zhang on 16/10/13.
 */
public class EmChatPullEntry {
    private static Logger logger = LoggerFactory.getLogger(EmChatPullEntry.class);
    private PropertiesConfiguration conf;
    private String cursor;
    private int cursorNum;
    private long totalNum;
    MongoCollection<Document> mongoCollection;
    private EasemobRestAPIFactory factory;

    public EmChatPullEntry(){
        MongoUtil mongoUtil = new MongoUtil();
        factory = ClientContext.getInstance().init(ClientContext.INIT_FROM_PROPERTIES).getAPIFactory();

        try {
            conf = new PropertiesConfiguration("ptb.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        this.cursor = conf.getString("gaia.emchat.cursor", null);
        mongoCollection = mongoUtil.mongoDatabase.getCollection("emchat");
    }

    public int pullTask(){
        ChatMessageAPI chat = (ChatMessageAPI)factory.newInstance(EasemobRestAPIFactory.MESSAGE_CLASS);
        ResponseWrapper responseWrapper = null;

        while (true){
            Object messages = chat.exportChatMessages(1000l, cursor, "select+*+where+timestamp>1403164734226");
            if (messages == null){
                logger.error("messages is null");
                break;
            }
            responseWrapper = (ResponseWrapper) messages;

            if (responseWrapper.getResponseStatus() != 200){
                if (responseWrapper.getResponseStatus() == 429 || responseWrapper.getResponseStatus() == 503){
                    logger.error("Request too fast 10/m");
                    break;
                }
                logger.error("{},{}", responseWrapper.getResponseStatus(), ((ObjectNode) responseWrapper.getResponseBody()).get("error_description").toString());
                return responseWrapper.getResponseStatus();
            }

            ObjectNode objectNode = (ObjectNode)responseWrapper.getResponseBody();
            if (objectNode == null){
                logger.error("objectNode is null");
                break;
            }
            ArrayNode arrayNode = (ArrayNode) objectNode.get("entities");
            if (arrayNode == null){
                logger.error("arrayNode is null");
                break;
            }

            for (int i = cursorNum;i < arrayNode.size(); i++){
                Document document = Document.parse(arrayNode.get(i).toString());
                document.put("_id", document.get("msg_id"));
                document.remove("msg_id");
                this.insertOneMongo(document);
                totalNum++;
            }
            if (objectNode.get("cursor") == null){
                cursorNum = arrayNode.size();
                break;
            }
            cursorNum = 0;
            cursor = ((TextNode) objectNode.get("cursor")).toString();
            logger.error("cursor_log: {} / total_num: {}", cursor, totalNum);
        }
        return responseWrapper.getResponseStatus();
    }


    public void insertIntoMongo(List<Document> documents){
        if (documents == null || documents.size() == 0){
            return;
        }
        try {
            mongoCollection.insertMany(documents);
        }catch (Exception e){
            logger.error("{}",documents.toString(),e);
        }
    }

    public void insertOneMongo(Document doc){
        if (doc == null){
            return;
        }
        try {
            mongoCollection.insertOne(doc);
        }catch (Exception e){
            logger.error("{}",doc.toJson(),e);
        }

    }

    public void cursorFindMongo(){
        MongoCursor<Document> iterator = mongoCollection.find().iterator();
        while (iterator.hasNext()){
            iterator.next();
        }
    }

    public static void emchatExport(){

        long internal = 60000; //间隔时间毫秒
        EmChatPullEntry EmChatPullEntry = new EmChatPullEntry();
        while (true){
            EmChatPullEntry.pullTask();

            try {
                Thread.sleep(internal);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args){
        EmChatPullEntry EmChatPullEntry = new EmChatPullEntry();
        EmChatPullEntry.pullTask();
    }
}
