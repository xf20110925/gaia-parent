package com.ptb.gaia.service.service;

import com.alibaba.fastjson.JSON;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOptions;
import com.ptb.gaia.service.entity.media.GLiveMediaCase;
import com.ptb.gaia.service.entity.media.GLiveMediaStatic;
import org.apache.commons.configuration.ConfigurationException;
import org.bson.Document;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by liujianpeng on 2017/2/27 0027.
 */
public class GLiveMediaService extends GaiaServiceBasic {

    public GLiveMediaService() throws ConfigurationException {
        super();
    }

    Document ConvertGLiveMediaToDocument(GLiveMediaStatic liveMediaStatic){
        Document document = Document.parse(JSON.toJSONString(liveMediaStatic));
        document.put("updateTime", System.currentTimeMillis());
        return document;
    }


    Document ConvertGLiveArticleToDocument(GLiveMediaCase gLiveMediaCase){
        Document document = Document.parse(JSON.toJSONString(gLiveMediaCase));
        document.put("updateTime", System.currentTimeMillis());
        return document;
    }

    Document ConvertGLiveMediaToDocumentu(GLiveMediaStatic liveMediaStatic){
//        Document document2 = Document.parse(JSON.toJSONString(liveMediaStatic));
        Document document = new Document();
        document.put("maxLiveUserNum",liveMediaStatic.getMaxLiveUserNum());
        document.put("avgLiveUserNum",liveMediaStatic.getAvgLiveUserNum());
        document.put("totalReward",liveMediaStatic.getTotalReward());
        document.put("totalAmount",liveMediaStatic.getTotalAmount());
        document.put("totalLikeNum",liveMediaStatic.getTotalLikeNum());
        document.put("fansData", JSON.toJSONString(liveMediaStatic.getFansData()));
        document.put("updateTime", System.currentTimeMillis());
        return document;
    }

    public List<GLiveMediaStatic> getLiveMediaBatch(List<String> pmidList){
        return getMedias(getLiveMediaCollect(), pmidList, GLiveMediaStatic.class);
    }

    public Boolean insertBatch(List<GLiveMediaStatic> liveMedia){
        List<UpdateManyModel<Document>> list = liveMedia.stream().map(live -> {
            return new UpdateManyModel<Document>(new Document("_id", live.getPmid()), new Document("$set", ConvertGLiveMediaToDocument(live)), updateOptions);
        }).collect(Collectors.toList());
        getLiveMediaCollectByPrimary().bulkWrite(list);
        return true;

    }


    public Boolean updateBatch(List<GLiveMediaStatic> liveMedia){
        List<UpdateManyModel<Document>> list = liveMedia.stream().map(live -> {
            return new UpdateManyModel<Document>(new Document("pmid", live.getPmid()), new Document("$set", ConvertGLiveMediaToDocumentu(live)),  new UpdateOptions().upsert(false));
        }).collect(Collectors.toList());
        getLiveMediaCollectByPrimary().bulkWrite(list);
        return true;

    }



    public Boolean insertArticleBatch(List<GLiveMediaCase> liveMediaCases){
        List<UpdateManyModel<Document>> list = liveMediaCases.stream().map(live -> {
            return new UpdateManyModel<Document>(new Document("_id", live.getScid()),new Document("$set", ConvertGLiveArticleToDocument(live)), updateOptions);
        }).collect(Collectors.toList());
        getLiveArticleCollectByPrimary().bulkWrite(list);
        return true;
    }

}
