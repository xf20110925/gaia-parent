package com.ptb.gaia.service.service;

import com.alibaba.fastjson.JSON;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateManyModel;
import com.ptb.gaia.service.ArticleRankType;
import com.ptb.gaia.service.entity.article.GArticleSet;
import com.ptb.gaia.service.entity.article.GWbArticle;
import com.ptb.gaia.service.entity.article.GWbArticleBasic;
import com.ptb.gaia.service.entity.article.GWbArticleDynamic;
import com.ptb.gaia.service.utils.JedisUtil;
import com.ptb.utils.string.RegexUtils;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.ptb.gaia.service.ArticleRankType.RANK_INVOLVE_NUM_WB;
import static com.ptb.gaia.service.ArticleRankType.RANK_POSTTIME_WB;

/**
 * Created by eric on 16/6/23.
 */
public class GWbArticleService extends GaiaServiceBasic {

    public static final String HOT_ARTICLE_PREFIX = "article:hot:wb";

    public GWbArticleService() throws ConfigurationException {
        super();
    }

    Document convertGWbBasicToDocument(GWbArticleBasic wbArticleBasic) {
        Document doc = Document.parse(JSON.toJSONString(wbArticleBasic));
        doc.remove("commentNum");
        doc.remove("likeNum");
        doc.remove("forwardNum");
        doc.remove("zpzNum");
        doc.put("_id", wbArticleBasic.getArticleUrl());
        return doc;
    }

    Document convertGWbBasicDynamicToDocument(GWbArticleDynamic wbArticleBasic) {
        Document doc = Document.parse(JSON.toJSONString(wbArticleBasic));
        doc.remove("addTime");
        doc.put("updateTime", System.currentTimeMillis());
        doc.put("pmid", RegexUtils.sub("http://.*.weibo.cn/([^/]+)/.*", wbArticleBasic.getArticleUrl(), 0));
        doc.put("zpzNum", wbArticleBasic.getForwardNum() + wbArticleBasic.getCommentNum() + wbArticleBasic.getLikeNum());
        if (StringUtils.isBlank(doc.getString("pmid"))) doc.remove("pmid");
        return doc;
    }

    Document convertGWbBasicDynamicPushToDocument(GWbArticleDynamic wbArticleBasic) {
        Map map = new HashMap();
        map.put("forwardNum", wbArticleBasic.getForwardNum());
        map.put("likeNum", wbArticleBasic.getLikeNum());
        map.put("commentNum", wbArticleBasic.getCommentNum());

        try {
            map.put("zpzNum", wbArticleBasic.getForwardNum() + wbArticleBasic.getCommentNum() + wbArticleBasic.getLikeNum());
        }catch (Exception e) {
            e.printStackTrace();
            map.put("zpzNum", 0);
        }

        map.put("addTime", wbArticleBasic.getAddTime());
        return new Document("dynamics", map);
    }

    public Boolean insertBasicBatch(List<GWbArticleBasic> wbArticleBasics) {
//        MongoCollection<Document> wbArticleCollect = getWbArticleCollect();
        List<UpdateManyModel<Document>> list = wbArticleBasics.stream().map(article -> {
            return new UpdateManyModel<Document>(new Document("_id", article.getArticleUrl()), new Document("$set", convertGWbBasicToDocument(article)), updateOptions);
        }).collect(Collectors.toList());
        getWbArticleCollectByPrimary().bulkWrite(list);
        return true;
    }

    public Boolean inserDynamicBatch(List<GWbArticleDynamic> wbArticleDynamics) {
//        MongoCollection<Document> wbArticleCollect = getWbArticleCollect();
        List<UpdateManyModel<Document>> list = wbArticleDynamics.stream().map(m -> {
            Document set = new Document("$set", convertGWbBasicDynamicToDocument(m));
            set.put("$push", convertGWbBasicDynamicPushToDocument(m));
            Bson condition = Filters.eq("_id", m.getArticleUrl());
            return new UpdateManyModel<Document>(condition, set, updateOptions);
        }).collect(Collectors.toList());

        BulkWriteResult bulkWriteResult = getWbArticleCollectByPrimary().bulkWrite(list);
        return true;
    }

    private GArticleSet<GWbArticle> getWbArticles(List<String> pmids, int start, int end, ArticleRankType rankType) {
        if (pmids == null) {
            return GArticleSet.Null;
        }
        start = start >= 0 ? start : 0;
        end = end >= start ? end : start;

        String cacheKey = generateCachePmidRankKey(String.format("%s:%s:", HOT_ARTICLE_PREFIX, rankType.name()), pmids);
        long articleUrlSize = JedisUtil.zKeySize(cacheKey);
        List<String> hotArticleUrls = null;
        if (articleUrlSize == 0) {
            hotArticleUrls = getWbArticlesByMongo(pmids, rankType.getSortFields(),rankType);
            cacheMedia(cacheKey, hotArticleUrls);
            hotArticleUrls = hotArticleUrls.stream().skip(start).limit(end - start).collect(Collectors.toList());
        } else {
            hotArticleUrls = JedisUtil.zget(cacheKey, start, end - 1).stream().collect(Collectors.toList());
        }

        List<GWbArticle> ret = getWbArticles(hotArticleUrls, rankType.getComparator());
        long totalNum = JedisUtil.zKeySize(cacheKey);
        return new GArticleSet<>((int) totalNum, ret);
    }




    private List<GWbArticle> getWbArticles(List<String> urls, Comparator<GWbArticle> comparator) {
        MongoCollection<Document> wbArticleCollect = getWbArticleCollect();
        FindIterable<Document> docs = wbArticleCollect.find(Filters.in("_id", urls));
        Set<GWbArticle> wxArticles = convertDocsToEntity(docs, GWbArticle.class);
        List<GWbArticle> rets = wxArticles.stream().sorted(comparator).collect(Collectors.toList());
        if (rets == null) {
            rets = new ArrayList();
        }
        return rets;
    }

    private List<String> getWbArticlesByMongo(List<String> pmids, List<String> sortFeilds, ArticleRankType articleRankType) {
        int limit = 100;
        switch (articleRankType) {
            case RANK_INVOLVE_NUM_WB:
            case RANK_INVOLVE_NUM_WX:
                limit = HOT_ARTICLE_BATCH_SIZE;
                break;
            case RANK_POSTTIME_WB:
            case RANK_POSTTIME_WX:
                limit = RECENT_ARTICLE_BATCH_SIZE;
                break;
        }

        MongoCollection<Document> wbArticleCollect = getWbArticleCollect();
        Document fieldsDoc = new Document("_id", 1);
        sortFeilds.forEach(item->{fieldsDoc.append(item, 1);});
        Bson filter = Filters.and(Filters.exists("title"),Filters.exists("likeNum"), Filters.in("pmid", pmids));
        FindIterable<Document> docs = wbArticleCollect.find(filter).projection(fieldsDoc);
        docs = docs.sort(Sorts.descending(sortFeilds)).limit(limit);
        List<String> ret = new LinkedList<>();
        for (Document doc : docs) ret.add(doc.getString("_id"));
        return ret;
    }

    public GArticleSet<GWbArticle> getHotArticles(List<String> pmids, int start, int end) {
        return getWbArticles(pmids, start, end, RANK_INVOLVE_NUM_WB);
    }

    public GArticleSet<GWbArticle> getRecentArticles(List<String> pmids, int start, int end) {
        return getWbArticles(pmids, start, end, RANK_POSTTIME_WB);
    }



    public int getWbRecentArticleCount(List<String> pmids) {
            if (pmids == null) {
                return 0;
            }
            String cacheKey = generateCachePmidRankKey(String.format("%s:%s:", RANK_POSTTIME_WB, RANK_POSTTIME_WB.name()), pmids);

            long articleUrlSize = JedisUtil.zKeySize(cacheKey);

            List<String> hotArticleUrls = null;
            if (articleUrlSize == 0) {
                hotArticleUrls = getWbArticlesByMongo(pmids,RANK_POSTTIME_WB.getSortFields(), RANK_POSTTIME_WB);
                cacheMedia(cacheKey, hotArticleUrls);
            }else{
                return Math.toIntExact(articleUrlSize);
            }
            return Math.toIntExact(articleUrlSize == 0 ? JedisUtil.zKeySize(cacheKey) : articleUrlSize);
    }
}
