package com.ptb.gaia.service;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.ptb.gaia.service.entity.article.*;
import com.ptb.gaia.service.entity.media.*;
import com.ptb.gaia.service.entity.article.GArticleSet;
import com.ptb.gaia.service.entity.article.GWbArticle;
import com.ptb.gaia.service.entity.article.GWbArticleBasic;
import com.ptb.gaia.service.entity.article.GWbArticleDynamic;
import com.ptb.gaia.service.entity.article.GWxArticle;
import com.ptb.gaia.service.entity.article.GWxArticleBasic;
import com.ptb.gaia.service.entity.article.GWxArticleDynamic;
import org.apache.xpath.operations.Bool;
import org.bson.Document;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.util.ArrayList;
import java.util.List;


/**
 *
 */
public interface IGaia {

    /**
     * @param biz
     * @return
     */
    public GWxMedia getWxMedia(String biz);

    public List<GWxMedia> getWxMediaBatch(List<String> biz);

    public List<GWbMedia> getWbMediaBatch(List<String> weiboids);

    List<GWxArticleDynamic> getWxDynamics(String articleUrl, Long startTime, Long endTime);

    public GWbMedia getWbMedia(String wbId);

    public GArticleSet<GWxArticle> getWxHotArticles(List<String> pmids, int start, int end);

    public GArticleSet<GWbArticle> getWbHotArticles(List<String> pmids, int start, int end);

    public GArticleSet<GWbArticle> getWbRecentArticles(List<String> pmids, int start, int end);

    public GArticleSet<GWxArticle> getWxRecentArticles(List<String> pmids, int start, int end);

    public int getWxRecentArticleCount(List<String> pmids);
    public int getWbRecentArticleCount(List<String> pmids);

    public GMediaSet<GWxMedia> getWxCapabilityMedia(List<String> pmids, int start, int end, RankType rankType);

    public GMediaSet<GWbMedia> getWbCapabilityMedia(List<String> pmids, int start, int end, RankType rankType);

    public GMediaSet<GWxMedia> getWxHotMedia(List<String> pmids, int start, int end, RankType rankType);

    public GMediaSet<GWbMedia> getWbHotMedia(List<String> pmid, int start, int end, RankType rankType);

    public Boolean addWxMediaBasicBatch(List<GWxMediaBasic> wxMedias);

    public Boolean addWxArticleBasicBatch(List<GWxArticleBasic> wxArticleBasics);

    public Boolean addWbMediaBasicBatch(List<GWbMediaBasic> wbMediaBasics);

    public Boolean addWbArticleDynamics(List<GWbArticleDynamic> wbArticleDynamic);

    public Boolean addWxArticleDynamics(List<GWxArticleDynamic> wxArticleDynamic);

    public Boolean addWbArticleBasicBatch(List<GWbArticleBasic> gWbArticleBasics);

    public GWxMedia getWxMediaByArticleUrl(String articleUrl);

    public GWbMedia getWbMediaByArticleUrl(String articleUrl);

    public List<GWbArticle> getWbArticle(List<String> articleUrls);
    public List<GWxArticle> getWxArtcile(List<String> articleUrls);

    public Document getWxDocument(WxMediaStatistics wxMediaStatistics);

    public Document getWbDocument(WbMediaStatistics wbMediaStatistics);

    public Boolean addBatchCategory(List<RecommendCategoryMedia> recommendCategoryMedias);
    public void removeAllRecommandCategory();
    public ArrayList<Document> getCovertType(ArrayList<Document> list);

    public ArrayList<BasicDBObject> getListBasicObject(FindIterable<Document> docs);

    Boolean addWxMediaDynamicBatch(List<GWxMediaBasic> gWxMediaBasics);
    public Boolean addLiveMediaBasicBatch(List<GLiveMediaStatic> gLiveMediaStatics);

    public Boolean addVedioMediaBasicBatch(List<GVedioMediaStatic> gVedioMediaStatics);

    public Boolean updateLiveMediaDynamicBatch(List<GLiveMediaStatic> gLiveMediaStatics);

    public List<GLiveMediaStatic> getLiveMediaBatch(List<String> pmidList);

    public Boolean addLiveArticleDynamicBatch(List<GLiveMediaCase> gLiveMediaCases);

    public List<String> getWxArticleByPmid(List<String> pmids);

    public Boolean updataWxMediaStatic(List<GWxMediaBasic> medias);

}