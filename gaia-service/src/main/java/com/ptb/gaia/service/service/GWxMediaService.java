package com.ptb.gaia.service.service;

import com.alibaba.fastjson.JSON;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.ptb.gaia.service.RankType;
import com.ptb.gaia.service.entity.media.GMediaSet;
import com.ptb.gaia.service.entity.media.GWxMedia;
import com.ptb.gaia.service.entity.media.GWxMediaBasic;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by eric on 16/6/23.
 */
public class GWxMediaService extends GaiaServiceBasic {
    private static final String RANK_WX_MEDIA_PREFIX = "rank:media:wx";


    public GWxMediaService() throws ConfigurationException {
    }

    public Document convertGWxBasicToDocument(GWxMediaBasic gwxMediaBasic) {
        Document doc = Document.parse(JSON.toJSONString(gwxMediaBasic));
        doc.put("_id", gwxMediaBasic.getPmid());

        if (StringUtils.isBlank(gwxMediaBasic.getAuthInfo())) {
            doc.remove("authInfo");
            doc.remove("isAuth");
        }

        if (StringUtils.isBlank(gwxMediaBasic.getWeixinId())) {
            doc.remove("qrcode");
            doc.remove("weixinID");
            doc.remove("headImage");
        }
//        doc.remove("updateTime");
        return doc;
    }

    public Document convertGWxDynamicToDocument(GWxMediaBasic gwxMediaBasic) {
        Document doc = new Document();
        doc.put("weixinId", gwxMediaBasic.getWeixinId());
        doc.put("fansNum", gwxMediaBasic.getFansNum());
        doc.put("updateTime", System.currentTimeMillis());
        return doc;
    }

    public Document convertGWxStaticToDocument(GWxMediaBasic gWxMediaBasic) {
        Document doc = new Document();
        doc.put("brief",gWxMediaBasic.getBrief());
        doc.put("headImage", gWxMediaBasic.getHeadImage());
        doc.put("mediaName", gWxMediaBasic.getMediaName());
        doc.put("qrcode",gWxMediaBasic.getQrcode());
        doc.put("updateTime", System.currentTimeMillis());
        doc.put("weixinId",gWxMediaBasic.getWeixinId());
        return doc;
    }

    public Boolean insert(GWxMediaBasic wxMediaBasic) {
        Document updateSet = new Document("$set", convertGWxBasicToDocument(wxMediaBasic));
        UpdateResult id = getWxMediaCollectByPrimary().updateOne(Filters.eq("_id", wxMediaBasic.getPmid()), updateSet, updateOptions);
        return id.getModifiedCount() > 0;
    }

    public Boolean insertBatch(List<? extends GWxMediaBasic> wxMedias) {
        List<UpdateManyModel<Document>> list = wxMedias.stream().map(m -> {
            return new UpdateManyModel<Document>(new Document("_id", m.getPmid()), new Document("$set", convertGWxBasicToDocument(m)), updateOptions);
        }).collect(Collectors.toList());
        getWxMediaCollectByPrimary().bulkWrite(list);
        return true;
    }

    public List<GWxMedia> getWxMedia(List<String> retPmids) {
        return getMedias(getWxMediaCollect(), retPmids, GWxMedia.class);
    }

    public GMediaSet<GWxMedia> getCapabilityMedia(List<String> pmids, int start, int end, RankType rankType) {
        return getMediaRank(pmids, start, end, rankType, MaxCapacityMediaNum, (pmid, rankType1, maxNum) -> {
            List<String> rankpmid = getMediaHotRankFromMongoDB(getWxMediaCollect(), pmids, rankType1, maxNum);
            return rankpmid;
        }, RANK_WX_MEDIA_PREFIX, getWxMediaCollect(), GWxMedia.class);
    }

    public Boolean insertWxAnalysisBatch(List<Document> wbAnalysisData) {
        List<UpdateManyModel<Document>> list = wbAnalysisData.stream().map(m -> {
            return new UpdateManyModel<Document>(new Document("_id", m.get("pmid")), new Document("$set", m), updateFalseOptions);
        }).collect(Collectors.toList());
        getWxMediaCollectByPrimary().bulkWrite(list);
        return true;
    }

    public Boolean insertWxHotWordBatch(List<Document> wxAnalysisData) {
        List<UpdateManyModel<Document>> list = wxAnalysisData.stream().map(m -> {
            return new UpdateManyModel<Document>(new Document("_id", m.get("pmid")), new Document("$set", m), updateFalseOptions);
        }).collect(Collectors.toList());
        getWxMediaCollectByPrimary().bulkWrite(list);
        return true;
    }


    public GMediaSet<GWxMedia> getHotMediaRank(List<String> pmids, int start, int end, RankType rankType) {
        return getMediaRank(pmids, start, end, rankType, MaxHotMediaNum, (pmid, rankType1, maxNum) -> {
            List<String> rankpmid = getMediaHotRankFromMongoDB(getWxMediaCollect(), pmids, rankType1, maxNum);
            return rankpmid;
        }, RANK_WX_MEDIA_PREFIX, getWxMediaCollect(), GWxMedia.class);
    }

    public Boolean insertDynamicBatch(List<GWxMediaBasic> gWxMediaBasics) {
        List<UpdateManyModel<Document>> list = gWxMediaBasics.stream().map(m ->
            new UpdateManyModel<Document>(new Document("weixinId", m.getWeixinId()), new Document("$set", convertGWxDynamicToDocument(m)), new UpdateOptions().upsert(false))
        ).collect(Collectors.toList());
        getWxMediaCollectByPrimary().bulkWrite(list);
        return true;
    }

    public Boolean updateManyModelWxMedia(List<GWxMediaBasic> medias){
        List<UpdateManyModel<Document>> collect = medias.stream().map(m -> {
            return new UpdateManyModel<Document>(new Document("_id", m.getPmid()), new Document("$set", convertGWxStaticToDocument(m)), new UpdateOptions().upsert(false));
        }).collect(Collectors.toList());
        getWxMediaCollectByPrimary().bulkWrite(collect);
        return true;
    }
}
