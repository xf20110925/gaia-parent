package com.ptb.gaia.index;

import com.ptb.gaia.index.bean.EsInsertData;
import com.ptb.gaia.index.es.ArticleConvert;
import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by watson zhang on 2017/3/20.
 */
public class EsArticleIndexImp extends EsIndexAbstract{
    private static Logger logger = LoggerFactory.getLogger(EsArticleIndexImp.class);

    ArticleConvert articleConvert;

    public EsArticleIndexImp(){
       articleConvert = new ArticleConvert();
    }

        @Override
    public int insertWxArticleTitleOne(EsInsertData esInsertData) {
        Map<String, Object> titleBuilder = articleConvert.convertDocToTitleObject(esInsertData.getJsonObject(), 1);
        if (titleBuilder == null){
            return -1;
        }
        indexServerEs.insert(articleTitleIndex, wxArticleType, esInsertData.getId(), titleBuilder);
        return 0;
    }

    @Override
    public int insertWbArticleTitleOne(EsInsertData esInsertData) {
        Map<String, Object> titleBuilder = articleConvert.convertDocToTitleObject(esInsertData.getJsonObject(), 2);
        if (titleBuilder == null){
            return -1;
        }
        indexServerEs.insert(articleTitleIndex, wbArticleType, esInsertData.getId(), titleBuilder);
        return 0;
    }

    @Override
    public int insertWxArticleTitleBatch(List<EsInsertData> esInsertDatas) {
        esInsertDatas.forEach(jsonObject -> {
            Map<String, Object> titleBuilder = articleConvert.convertDocToTitleObject(jsonObject.getJsonObject(), 1);
            if (titleBuilder == null){
                return;
            }
            indexServerEs.addMsg(titleBuilder, articleTitleIndex, wxArticleType, jsonObject.getId());
        });
        return 0;
    }

    @Override
    public int insertWbArticleTitleBatch (List<EsInsertData> esInsertDatas) {
        esInsertDatas.forEach(jsonObject -> {
            Map<String, Object> titleBuilder = articleConvert.convertDocToTitleObject(jsonObject.getJsonObject(), 2);
            if (titleBuilder == null){
                return;
            }
            indexServerEs.addMsg(titleBuilder, articleTitleIndex, wbArticleType, jsonObject.getId());
        });
        return 0;
    }

    @Override
    public int insertWxArticleContentOne(EsInsertData esInsertData) {
        Map<String, Object> contentBuilder = articleConvert.convertDocToContentObject(esInsertData.getJsonObject(), 1);
        if (contentBuilder == null){
            return -1;
        }
        indexServerEs.insert(articleContentIndex, wxArticleType, esInsertData.getId(), contentBuilder);
        return 0;
    }

    @Override
    public int insertWbArticleContentOne(EsInsertData esInsertData) {
        Map<String, Object> contentBuilder = articleConvert.convertDocToContentObject(esInsertData.getJsonObject(), 2);
        if (contentBuilder == null){
            return -1;
        }
        indexServerEs.insert(articleContentIndex, wbArticleType, esInsertData.getId(), contentBuilder);
        return 0;
    }

    @Override
    public int insertWxArticleContentBatch(List<EsInsertData> esInsertDatas) {
        esInsertDatas.forEach(jsonObject -> {
            Map<String, Object> contentBuilder = articleConvert.convertDocToContentObject(jsonObject.getJsonObject(), 1);
            if (contentBuilder == null){
                return;
            }
            indexServerEs.addMsg(contentBuilder, articleContentIndex, wxArticleType, jsonObject.getId());
        });
        return 0;
    }

    @Override
    public int insertWbArticleContentBatch(List<EsInsertData> esInsertDatas) {
        esInsertDatas.forEach(jsonObject -> {
            Map<String, Object> contentBuilder = articleConvert.convertDocToContentObject(jsonObject.getJsonObject(), 2);
            if (contentBuilder == null){
                return;
            }
            indexServerEs.addMsg(contentBuilder, articleContentIndex, wbArticleType, jsonObject.getId());
        });
        return 0;
    }

    @Override
    public int upsertWxArticleTitleBatch(List<EsInsertData> esInsertDatas) {
        if (esInsertDatas == null){
            return -1;
        }
        esInsertDatas.forEach(json -> {
            Map<String, Object> wxArticleData = articleConvert.convertDocToTitleObject(json.getJsonObject(), 1);
            if (wxArticleData == null){
                logger.warn("upsert wx article batch error! esId:{}", json.getId());
                return;
            }
            indexServerEs.upsertNotScore(articleTitleIndex, wxArticleType, json.getId(), wxArticleData);
        });
        return 0;
    }

    @Override
    public int upsertWxArticleContentBatch(List<EsInsertData> esInsertDatas) {
        if (esInsertDatas == null){
            return -1;
        }
        esInsertDatas.forEach(json -> {
            Map<String, Object> wxArticleData = articleConvert.convertDocToContentObject(json.getJsonObject(), 1);
            if (wxArticleData == null){
                logger.warn("upsert wx article batch error! esId:{}", json.getId());
                return;
            }
            indexServerEs.upsertNotScore(articleContentIndex, wxArticleType, json.getId(), wxArticleData);
        });
        return 0;
    }

    @Override
    public int upsertWbArticleTitleBatch(List<EsInsertData> esInsertDatas) {
        if (esInsertDatas == null){
            return -1;
        }
        esInsertDatas.forEach(json -> {
            Map<String, Object> wbArticleData = articleConvert.convertDocToTitleObject(json.getJsonObject(), 2);
            if (wbArticleData == null){
                logger.warn("upsert wb article batch error! esId:{}", json.getId());
                return;
            }
            indexServerEs.upsertNotScore(articleTitleIndex, wbArticleType, json.getId(), wbArticleData);
        });
        return 0;
    }

    @Override
    public int upsertWbArticleContentBatch(List<EsInsertData> esInsertDatas) {
        if (esInsertDatas == null){
            return -1;
        }
        esInsertDatas.forEach(json -> {
            Map<String, Object> wbArticleData = articleConvert.convertDocToContentObject(json.getJsonObject(), 2);
            if (wbArticleData == null){
                logger.warn("upsert wb article batch error! esId:{}", json.getId());
                return;
            }
            indexServerEs.upsertNotScore(articleContentIndex, wbArticleType, json.getId(), wbArticleData);
        });
        return 0;
    }

    @Override
    public int insertWxArticleTitleFromMongo(EsInsertData esInsertData) {
        if (esInsertData == null){
            return -1;
        }
        HashedMap hashedMap = articleConvert.mongoToEsTitle(esInsertData.getJsonObject(), 1);
        if (hashedMap == null){
            logger.warn("insert wx article title error! esId:{}", esInsertData.getId());
            return -1;
        }
        indexServerEs.addMsg(hashedMap, articleTitleIndex, wxArticleType, esInsertData.getId());
        return 0;
    }

    @Override
    public int insertWxArticleContentFromMongo(EsInsertData esInsertData) {
        if (esInsertData == null){
            return -1;
        }
        HashedMap hashedMap = articleConvert.mongoToEsContent(esInsertData.getJsonObject(), 1);
        if (hashedMap == null){
            logger.warn("insert wx article content error! esId:{}", esInsertData.getId());
            return -1;
        }
        indexServerEs.addMsg(hashedMap, articleContentIndex, wxArticleType, esInsertData.getId());
        return 0;
    }

    @Override
    public int insertWbArticleTitleFromMongo(EsInsertData esInsertData) {
        if (esInsertData == null){
            return -1;
        }
        HashedMap hashedMap = articleConvert.mongoToEsTitle(esInsertData.getJsonObject(), 2);
        if (hashedMap == null){
            logger.warn("insert wx article title error! esId:{}", esInsertData.getId());
            return -1;
        }
        indexServerEs.addMsg(hashedMap, articleTitleIndex, wbArticleType, esInsertData.getId());
        return 0;
    }

    @Override
    public int insertWbArticleContentFromMongo(EsInsertData esInsertData) {
        if (esInsertData == null){
            return -1;
        }
        HashedMap hashedMap = articleConvert.mongoToEsContent(esInsertData.getJsonObject(), 2);
        if (hashedMap == null){
            logger.warn("insert wx article content error! esId:{}", esInsertData.getId());
            return -1;
        }
        indexServerEs.addMsg(hashedMap, articleContentIndex, wbArticleType, esInsertData.getId());
        return 0;
    }

    @Override
    public void flushEsBulk() {
        indexServerEs.flushEsBulk();
    }
}
