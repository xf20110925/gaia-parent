package com.ptb.gaia.service.service;

import com.alibaba.fastjson.JSON;
import com.mongodb.client.model.UpdateManyModel;
import com.ptb.gaia.service.RankType;
import com.ptb.gaia.service.entity.media.GMediaSet;
import com.ptb.gaia.service.entity.media.GWbMedia;
import com.ptb.gaia.service.entity.media.GWbMediaBasic;
import org.apache.commons.configuration.ConfigurationException;
import org.bson.Document;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by eric on 16/6/23.
 */
public class GWbMediaService extends GaiaServiceBasic {
    private static final String RANK_WB_MEDIA_PREFIX = "rank:media:wb";

    Document convertGWbBasicToDocument(GWbMediaBasic gwbMediaBasic) {
        Document doc;
        if (gwbMediaBasic.getFansNum() >= 0) {
            doc = new Document();
            doc.put("fansNum", gwbMediaBasic.getFansNum());
            doc.put("postNum", gwbMediaBasic.getPostNum());
        } else {
            doc = Document.parse(JSON.toJSONString(gwbMediaBasic));
            doc.remove("fansNum");
            doc.remove("postNum");
            doc.remove("active");
        }
        doc.put("_id", gwbMediaBasic.getPmid());
//        doc.remove("updateTime");
        return doc;
    }

    public GWbMediaService() throws ConfigurationException {
        super();
    }

    public Boolean insertBatch(List<GWbMediaBasic> wbMediaBasics) {
        List<UpdateManyModel<Document>> list = wbMediaBasics.stream().map(m -> {
            return new UpdateManyModel<Document>(new Document("_id", m.getPmid()), new Document("$set", convertGWbBasicToDocument(m)), updateOptions);
        }).collect(Collectors.toList());
        getWbMediaCollectByPrimary().bulkWrite(list);
        return true;
    }


    public Boolean insertWbAnalysisBatch(List<Document> wbAnalysisData) {
        List<UpdateManyModel<Document>> list = wbAnalysisData.stream().map(m -> {
            return new UpdateManyModel<Document>(new Document("_id", m.get("pmid")), new Document("$set", m), updateFalseOptions);
        }).collect(Collectors.toList());
        getWbMediaCollectByPrimary().bulkWrite(list);
        return true;
    }

    public Boolean insertWbHotWordBatch(List<Document> wbAnalysisData) {
        List<UpdateManyModel<Document>> list = wbAnalysisData.stream().map(m -> {
            return new UpdateManyModel<Document>(new Document("_id", m.get("pmid")), new Document("$set", m), updateFalseOptions);
        }).collect(Collectors.toList());
        getWbMediaCollectByPrimary().bulkWrite(list);
        return true;
    }

    public List<GWbMedia> getWbMedia(List<String> retPmids) {
        return getMedias(getWbMediaCollect(), retPmids, GWbMedia.class);
    }

    public GMediaSet<GWbMedia> getCapabilityMedia(List<String> pmids, int start, int end, RankType rankType) {

        return getMediaRank(pmids, start, end, rankType, MaxCapacityMediaNum, (pmid, rankType1, maxNum) -> {
            List<String> rankpmid = getMediaHotRankFromMongoDB(getWbMediaCollect(), pmids, rankType1, maxNum);
            return rankpmid;
        }, RANK_WB_MEDIA_PREFIX, getWbMediaCollect(), GWbMedia.class);
    }

    public GMediaSet<GWbMedia> getHotMediaRank(List<String> pmids, int start, int end, RankType rankType) {
        return getMediaRank(pmids, start, end, rankType, MaxHotMediaNum, (pmids1, rankType1, maxNum) -> {
            List<String> rankpmid = getMediaHotRankFromMongoDB(getWbMediaCollect(), pmids1, rankType1, maxNum);
            return rankpmid;
        }, RANK_WB_MEDIA_PREFIX, getWbMediaCollect(), GWbMedia.class);
    }
}
