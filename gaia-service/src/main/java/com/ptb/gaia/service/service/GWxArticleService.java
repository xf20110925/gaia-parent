package com.ptb.gaia.service.service;

import com.alibaba.fastjson.JSON;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOneModel;
import com.ptb.gaia.service.ArticleRankType;
import com.ptb.gaia.service.entity.article.GArticleSet;
import com.ptb.gaia.service.entity.article.GWxArticle;
import com.ptb.gaia.service.entity.article.GWxArticleBasic;
import com.ptb.gaia.service.entity.article.GWxArticleDynamic;
import com.ptb.gaia.service.utils.JedisUtil;
import com.ptb.utils.string.RegexUtils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;
import java.util.stream.Collectors;

import static com.ptb.gaia.service.ArticleRankType.RANK_INVOLVE_NUM_WX;
import static com.ptb.gaia.service.ArticleRankType.RANK_POSTTIME_WX;


/**
 * Created by eric on 16/6/23.
 */
public class GWxArticleService extends GaiaServiceBasic {

    public static final String WX_ARTICLE_PREFIX = "article:hot:wx";

    public GWxArticleService() throws ConfigurationException {
        super();
    }

    private Document convertGWxBasicToDocument(GWxArticleBasic wxArticleBasic) {
        Document doc = Document.parse(JSON.toJSONString(wxArticleBasic));
        doc.put("_id", wxArticleBasic.getArticleUrl());
        doc.remove("readNum");
        doc.remove("likeNum");
        return doc;
    }

    private Document convertGWxBasicDynamicToDocument(GWxArticleDynamic gWxArticleBasic) {
        Document doc = Document.parse(JSON.toJSONString(gWxArticleBasic));
        doc.remove("addTime");
        doc.put("updateTime", System.currentTimeMillis());
        doc.put("_id", gWxArticleBasic.getArticleUrl());
        doc.put("pmid", RegexUtils.sub("http://mp.weixin.qq.com/s\\?__biz=([^&]+)&mid.*", gWxArticleBasic.getArticleUrl(), 0));
        return doc;
    }

    private Document convertGWxBasicDynamicPushToDocument(GWxArticleDynamic gWxArticleBasic) {
        Map map = new HashMap();
        map.put("readNum", gWxArticleBasic.getReadNum());
        map.put("likeNum", gWxArticleBasic.getLikeNum());
        map.put("addTime", gWxArticleBasic.getAddTime());

        return new Document("dynamics", map);
    }


    public Boolean insertBasicBatch(List<GWxArticleBasic> gWxArticleBasics) {
        List<UpdateOneModel<Document>> list = gWxArticleBasics.stream().map(m -> {
            return new UpdateOneModel<Document>(new Document("_id", m.getArticleUrl()), new Document("$set", convertGWxBasicToDocument(m)), updateOptions);
        }).collect(Collectors.toList());
        getWxArticleCollectByPrimary().bulkWrite(list);
        return true;
    }

    public Boolean insertDynamicBatch(List<GWxArticleDynamic> gWxArticleBasics) {
        List<UpdateOneModel<Document>> list = gWxArticleBasics.stream().map(m -> {
            Document set = new Document("$set", convertGWxBasicDynamicToDocument(m));
            set.put("$push", convertGWxBasicDynamicPushToDocument(m));
            try {
                Bson condition = Filters.and(Filters.eq("pmid", RegexUtils.sub("http://mp.weixin.qq.com/s\\?__biz=([^&]+)&mid.*", m.getArticleUrl(), 0)),
                                             Filters.eq("_id", m.getArticleUrl()));
                return new UpdateOneModel<Document>(condition, set, updateOptions);
            }catch (Exception e) {
                return null;
            }
        }).filter(documentUpdateOneModel -> documentUpdateOneModel != null).collect(Collectors.toList());
        getWxArticleCollectByPrimary().bulkWrite(list);
        return true;
    }

    public int getWxRecentArticleCount(List<String> pmids) {
        if (pmids == null) {
            return 0;
        }
        String cacheKey = generateCachePmidRankKey(String.format("%s:%s:", WX_ARTICLE_PREFIX, RANK_POSTTIME_WX.name()), pmids);

        long articleUrlSize = JedisUtil.zKeySize(cacheKey);

        List<String> hotArticleUrls = null;
        if (articleUrlSize == 0) {
            hotArticleUrls = getWxArticlesByMongo(pmids,RANK_POSTTIME_WX.getSortFields(), RANK_POSTTIME_WX);
            cacheMedia(cacheKey, hotArticleUrls);
        }else{
            return Math.toIntExact(articleUrlSize);
        }

        return Math.toIntExact(articleUrlSize == 0 ? JedisUtil.zKeySize(cacheKey) : articleUrlSize);
    }

    private GArticleSet<GWxArticle> getWxArticles(List<String> pmids, int start, int end, ArticleRankType rankType) {
        if (pmids == null) {
            return GArticleSet.Null;
        }
        start = start >= 0 ? start : 0;
        end = end >= start ? end : start;


        String cacheKey = generateCachePmidRankKey(String.format("%s:%s:", WX_ARTICLE_PREFIX, rankType.name()), pmids);

        long articleUrlSize = JedisUtil.zKeySize(cacheKey);

        List<String> hotArticleUrls = null;
        if (articleUrlSize == 0) {
            hotArticleUrls = getWxArticlesByMongo(pmids, rankType.getSortFields(), rankType);
            cacheMedia(cacheKey, hotArticleUrls);
            hotArticleUrls = hotArticleUrls.stream().skip(start).limit(end - start).collect(Collectors.toList());
        } else {
            hotArticleUrls = JedisUtil.zget(cacheKey, start, end - 1).stream().collect(Collectors.toList());
        }

        //long l = System.currentTimeMillis();
        List<GWxArticle> requiredArticles = getWxArticles(hotArticleUrls, rankType.getComparator());
        //System.out.println(System.currentTimeMillis() - l);
        long totalNum = articleUrlSize == 0 ? JedisUtil.zKeySize(cacheKey) : articleUrlSize;
        GArticleSet<GWxArticle> retSet = new GArticleSet<>(Math.toIntExact(totalNum), requiredArticles);

        return retSet;
    }

    private List<GWxArticle> getWxArticles(List<String> urls, Comparator<GWxArticle> comparator) {
        MongoCollection<Document> wxArticleCollect = getWxArticleCollect();
        FindIterable<Document> docs = wxArticleCollect.find(Filters.in("_id", urls));
        Set<GWxArticle> wxArticles = convertDocsToEntity(docs, GWxArticle.class);
        List<GWxArticle> rets = wxArticles.stream().sorted(comparator).collect(Collectors.toList());
        if (rets == null) {
            return new ArrayList<>();
        }
        return rets;

    }

    private List<String> getWxArticlesByMongo(List<String> pmids, List<String> sortField, ArticleRankType rankType) {
        int limit = 100;
        Bson filter = new Document();
        switch (rankType) {
            case RANK_INVOLVE_NUM_WB:
            case RANK_INVOLVE_NUM_WX:
                filter = Filters.and(Filters.in("pmid", pmids),Filters.gt("postTime", getTimeAfterTondayMiddleNight(-30).getTime()),(Filters.exists("likeNum")));
                break;
            case RANK_POSTTIME_WB:
            case RANK_POSTTIME_WX:
                filter = Filters.and(Filters.in("pmid", pmids),Filters.gt("postTime", getTimeAfterTondayMiddleNight(-30).getTime()),(Filters.exists("likeNum")));
                break;
        }
        MongoCollection<Document> wxArticleCollect = getWxArticleCollect();
        Document fieldsDoc = new Document("_id", 1);
        sortField.forEach(x->{fieldsDoc.append(x,1);});
        FindIterable<Document> docs = wxArticleCollect.find(filter).projection(fieldsDoc);
        docs = docs.sort(Sorts.descending(sortField)).limit(200);
        List<String> ret = new LinkedList<>();
        for (Document doc : docs) ret.add(doc.getString("_id"));
        return ret;
    }

    public GArticleSet<GWxArticle> getHotArticles(List<String> pmids, int start, int end) {
        return getWxArticles(pmids, start, end, RANK_INVOLVE_NUM_WX);
    }

    public GArticleSet<GWxArticle> getRecentArticles(List<String> pmids, int start, int end) {
        return getWxArticles(pmids, start, end, RANK_POSTTIME_WX);
    }

    public List<GWxArticleDynamic> getDynamics(String articleUrl, Long startTime, Long endTime) {
        Document doc = getWxArticleCollect().find(Filters.eq("_id", articleUrl)).projection(new Document("dynamics", 1).append("_id", 0)).first();
        List<GWxArticleDynamic> dynamics = JSON.parseArray(JSON.toJSONString(doc.get("dynamics")), GWxArticleDynamic.class);
        List<GWxArticleDynamic> ret = dynamics.stream().filter(dynamic -> dynamic.getAddTime() >= startTime && dynamic.getAddTime() <= endTime).sorted(Comparator.comparingLong(GWxArticleDynamic::getAddTime)).collect(Collectors.toList());
        return ret;
    }

    public List<String> getWxArticleList(List<String> pmids){
        MongoCollection<Document> wxArticleCollect = getWxArticleCollect();
        List<String> collect = pmids.stream().map(pmid -> {
            FindIterable<Document> docs = wxArticleCollect.find(Filters.eq("pmid", pmid)).limit(1).sort(Sorts.descending("postTime"));
            String articleUrl = null;
            for (Document document : docs) {
                articleUrl = document.get("articleUrl").toString();
            }
            return articleUrl;
        }).collect(Collectors.toList());
        return collect;
    }


    public static void main(String[] args) throws ConfigurationException {
        GWxArticleService service = new GWxArticleService();
  /*      Date timeAfterTondayMiddleNight = service.getTimeAfterTondayMiddleNight(8);
        System.out.println(timeAfterTondayMiddleNight.getTime());
        //        List<String> pmids = Arrays.asList("MTE3MzE4MTAyMQ==", "MzA4MDA5MzY0MA==", "MjM5NzUzMDM2OQ==");
//        service.getRecentArticles(pmids, 0, 10);
        List<GWxArticleDynamic> dynamics = service.getDynamics("http://mp.weixin.qq.com/s?__biz=MzIwOTE0NTI3OQ==&mid=2651531425&idx=3&sn=f36fabee5fab3692c4cceb838c092c1c#rd", 1466740150000L, 1466754623000L);
        dynamics.forEach(x -> System.out.print(JSON.toJSONString(x)));*/
        List<String> wxArticleList = service.getWxArticleList(Arrays.asList("MjA0MTg1NjMyMA==", "MzA3MDU5MzUwNg=="));
    }


}
