package com.ptb.gaia.index;

import com.ptb.gaia.index.bean.*;
import com.ptb.gaia.index.es.WebMediaSearchConvert;
import com.ptb.gaia.index.es.LiveMediaConvert;
import com.ptb.gaia.index.es.MediaConvert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by watson zhang on 2017/3/20.
 */
public class EsMediaIndexImp extends EsIndexAbstract{
    private static Logger logger = LoggerFactory.getLogger(EsMediaIndexImp.class);

    MediaConvert mediaConvert;
    LiveMediaConvert liveMediaConvert;
    WebMediaSearchConvert WebMediaSearchConvert;

    public EsMediaIndexImp(){
        mediaConvert = new MediaConvert();
        liveMediaConvert = new LiveMediaConvert();
        WebMediaSearchConvert = new WebMediaSearchConvert();
    }

    @Override
    public int insertWxMediaOne(EsInsertData esInsertData) {
        Map<String, Object> objectMap = mediaConvert.convertWxMedia(esInsertData.getJsonObject());
        if (objectMap == null){
            logger.warn("insert wx media one error! esId:{}", esInsertData.getId());
            return -1;
        }
        indexServerEs.insert(mediaIndex, wxMediaType, esInsertData.getId(), objectMap);
        return 0;
    }

    @Override
    public int insertWbMediaOne(EsInsertData esInsertData) {
        Map<String, Object> objectMap = mediaConvert.convertWbMedia(esInsertData.getJsonObject());
        if (objectMap == null){
            logger.warn("insert wb media one error! esId:{}", esInsertData.getId());
            return -1;
        }
        indexServerEs.insert(mediaIndex, wbMediaType, esInsertData.getId(), objectMap);
        return 0;
    }

    @Override
    public int insertWxMediaBatch(List<EsInsertData> esInsertDatas) {
        esInsertDatas.forEach(jsonObject -> {
            Map<String, Object> weixinMediaData = mediaConvert.convertWxMedia(jsonObject.getJsonObject());
            if (weixinMediaData == null){
                return;
            }
            indexServerEs.addMsg(weixinMediaData, mediaIndex, wxMediaType, jsonObject.getId());
        });
        return 0;
    }

    @Override
    public int insertWbMediaBatch(List<EsInsertData> esInsertDatas) {
        esInsertDatas.forEach(jsonObject -> {
            Map<String, Object> weiboMediaData = mediaConvert.convertWbMedia(jsonObject.getJsonObject());
            if (weiboMediaData == null){
                return;
            }
            indexServerEs.addMsg(weiboMediaData, mediaIndex, wbMediaType, jsonObject.getId());
        });
        return 0;
    }

    @Override
    public int getBulkNum() {
        return 0;
    }

    @Override
    public int upsertWxMediaOne(EsInsertData esInsertData) {
        Map<String, Object> weixinMediaData = mediaConvert.convertWxMedia(esInsertData.getJsonObject());
        if (weixinMediaData == null){
            logger.warn("insert wx media one error! esId:{}", esInsertData.getId());
            return -1;
        }
        indexServerEs.upsertMedia(mediaIndex, wxMediaType, "_id", esInsertData.getId(), weixinMediaData);
        return 0;
    }

    @Override
    public int upsertWbMediaOne(EsInsertData esInsertData) {
        Map<String, Object> weiboMediaData = mediaConvert.convertWbMedia(esInsertData.getJsonObject());
        if (weiboMediaData == null){
            logger.warn("insert wb media one error! esId:{}", esInsertData.getId());
            return -1;
        }
        indexServerEs.upsertMedia(mediaIndex, wbMediaType, "_id", esInsertData.getId(), weiboMediaData);
        return 0;
    }

    @Override
    public int upsertWxMediaBatch(List<EsInsertData> esInsertDatas){
        if (esInsertDatas == null){
            return -1;
        }
        esInsertDatas.forEach(json -> {
            Map<String, Object> wxMediaData = mediaConvert.convertWxMedia(json.getJsonObject());
            if (wxMediaData == null){
                logger.warn("upsert wx media batch error! esId:{}", json.getId());
                return;
            }
            indexServerEs.upsertMedia(mediaIndex, wxMediaType, "_id", json.getId(), wxMediaData);
        });
        return 0;
    }

    @Override
    public int upsertWbMediaBatch(List<EsInsertData> esInsertDatas){
        if (esInsertDatas == null){
            return -1;
        }
        esInsertDatas.forEach(json -> {
            Map<String, Object> wbMediaData = mediaConvert.convertWbMedia(json.getJsonObject());
            if (wbMediaData == null){
                logger.warn("upsert wb media batch error! esId:{}", json.getId());
                return;
            }
            indexServerEs.upsertMedia(mediaIndex, wbMediaType, "_id", json.getId(), wbMediaData);
        });
        return 0;
    }

    @Override
    public int insertWxMediaFromMongo(EsInsertData esInsertData) {
        if (esInsertData == null){
            return -1;
        }
        Map<String, Object> wxMediaObjectMap = mediaConvert.mongoConvertToEs(esInsertData.getJsonObject(), 1);
        if (wxMediaObjectMap == null){
            logger.warn("insert wx media error! esId:{}", esInsertData.getId());
            return -1;
        }
        indexServerEs.addMsg(wxMediaObjectMap, mediaIndex, wxMediaType, esInsertData.getId());
        return 0;
    }

    @Override
    public int insertWbMediaFromMongo(EsInsertData esInsertData) {
        if (esInsertData == null){
            return -1;
        }
        Map<String, Object> wbMediaObjectMap = mediaConvert.mongoConvertToEs(esInsertData.getJsonObject(), 2);
        if (wbMediaObjectMap == null){
            logger.warn("insert wx media error! esId:{}", esInsertData.getId());
            return -1;
        }
        indexServerEs.addMsg(wbMediaObjectMap, mediaIndex, wbMediaType, esInsertData.getId());
        return 0;
    }

    @Override
    public int upsertWxMediaFromMongo(EsInsertData esInsertData) {
        if (esInsertData == null){
            return -1;
        }
        Map<String, Object> wxMediaObjectMap = mediaConvert.mongoConvertToEs(esInsertData.getJsonObject(), 1);
        if (wxMediaObjectMap == null){
            logger.warn("insert wx media error! esId:{}", esInsertData.getId());
            return -1;
        }
        indexServerEs.upsertNotScore(mediaIndex, wxMediaType, esInsertData.getId(), wxMediaObjectMap);

        return 0;
    }

    @Override
    public int upsertWbMediaFromMongo(EsInsertData esInsertData) {
        if (esInsertData == null){
            return -1;
        }
        Map<String, Object> wbMediaObjectMap = mediaConvert.mongoConvertToEs(esInsertData.getJsonObject(), 2);
        if (wbMediaObjectMap == null){
            logger.warn("insert wb media error! esId:{}", esInsertData.getId());
            return -1;
        }
        indexServerEs.upsertNotScore(mediaIndex, wbMediaType, esInsertData.getId(), wbMediaObjectMap);

        return 0;
    }

    @Override
    public int insertLiveMediaFromMongo(EsInsertData esInsertData) {
        if (esInsertData == null){
            return -1;
        }
        Map<String, Object> liveMediaMap = liveMediaConvert.mongoConvertToEs(esInsertData.getJsonObject());
        if (liveMediaMap == null){
            logger.warn("");
            return -1;
        }
        indexServerEs.upsertNotScore(mediaIndex, liveMediaType, esInsertData.getId(), liveMediaMap);
        return 0;
    }

    @Override
    public int insertLiveMediaFromKafka(EsInsertData esInsertData){
         if (esInsertData == null){
            return -1;
        }
        Map<String, Object> liveMediaMap = liveMediaConvert.kafkaConvertToEs(esInsertData.getJsonObject());
        if (liveMediaMap == null){
            logger.warn("");
            return -1;
        }
        indexServerEs.upsertNotScore(mediaIndex, liveMediaType, esInsertData.getId(), liveMediaMap);
        return 0;
    }

    @Override
    public int insertWebSearchMedia(ReqMediaBasicIndex reqMediaBasicIndex) {
        if (reqMediaBasicIndex == null){
            return -1;
        }

        Map<String, Object> esMap = null;
        if (reqMediaBasicIndex.getPlatType() == 1){
            WxReqMediaIndex wxMediaReq = (WxReqMediaIndex) reqMediaBasicIndex;
            esMap = WebMediaSearchConvert.convertWxToEs(wxMediaReq);
            if (esMap == null){
                return -1;
            }
            indexServerEs.upsertNotScore(mediaIndex, wxMediaType, reqMediaBasicIndex.getPmid(), esMap);
        }else if (reqMediaBasicIndex.getPlatType() == 2){
            WbReqMediaIndex wbMediaReq = (WbReqMediaIndex) reqMediaBasicIndex;
            esMap = WebMediaSearchConvert.convertWbToEs(wbMediaReq);
            if (esMap == null){
                return -1;
            }
            indexServerEs.upsertNotScore(mediaIndex, wbMediaType, reqMediaBasicIndex.getPmid(), esMap);
        }else if (reqMediaBasicIndex.getPlatType() == 3){
            LiveReqMediaIndex liveMediaReq = (LiveReqMediaIndex) reqMediaBasicIndex;
            esMap = WebMediaSearchConvert.convertLiveToEs(liveMediaReq);
            if (esMap == null){
                return -1;
            }
            indexServerEs.upsertNotScore(mediaIndex, liveMediaType, reqMediaBasicIndex.getPmid(), esMap);
        }
        return 0;
    }

    @Override
    public void flushEsBulk() {
        indexServerEs.flushEsBulk();
    }
}
