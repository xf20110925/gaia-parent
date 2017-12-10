package com.ptb.gaia.index;

import com.ptb.gaia.index.bean.ReqMediaBasicIndex;
import com.ptb.gaia.index.bean.EsInsertData;

import java.util.List;

/**
 * Created by watson zhang on 2016/11/24.
 */
public interface IIndex {

    /**
     * 插入一条微信媒体数据
     * @param esInsertData
     * @return
     */
    int insertWxMediaOne(EsInsertData esInsertData);

    /**
     * 插入一条微博媒体数据
     * @param esInsertData
     * @return
     */
    int insertWbMediaOne(EsInsertData esInsertData);

    /**
     * 批量插入微信媒体数据
     * @param esInsertData
     * @return
     */
    int insertWxMediaBatch(List<EsInsertData> esInsertData);

    /**
     * 批量插入微博媒体数据
     * @param esInsertData
     * @return
     */
    int insertWbMediaBatch(List<EsInsertData> esInsertData);

    /**
     * 获取缓存中的数据条数
     * @return
     */
    int getBulkNum();

    /**
     * 更新并插入一条微信数据
     * @param esInsertData
     * @return
     */
    int upsertWxMediaOne(EsInsertData esInsertData);

    /**
     * 更新并插入一条微博媒体数据
     * @param esInsertData
     * @return
     */
    int upsertWbMediaOne(EsInsertData esInsertData);

    /**
     * 更新并批量插入微信媒体数据
     * @param esInsertDatas
     * @return
     */
    int upsertWxMediaBatch(List<EsInsertData> esInsertDatas);

    /**
     * 更新并批量插入微博媒体数据
     * @param esInsertDatas
     * @return
     */
    int upsertWbMediaBatch(List<EsInsertData> esInsertDatas);

    /**
     * 插入一条文章标题数据
     * @param esInsertData
     * @return
     */
    int insertWxArticleTitleOne(EsInsertData esInsertData);

    /**
     * 插入一条微博文章标题数据
     * @param esInsertData
     * @return
     */
    int insertWbArticleTitleOne(EsInsertData esInsertData);

    /**
     * 批量插入微信文章标题数据
     * @param esInsertData
     * @return
     */
    int insertWxArticleTitleBatch(List<EsInsertData> esInsertData);

    /**
     * 批量插入微博文章标题数据
     * @param esInsertData
     * @return
     */
    int insertWbArticleTitleBatch(List<EsInsertData> esInsertData);

    /**
     * 插入一条微信文章内容数据
     * @param esInsertData
     * @return
     */
    int insertWxArticleContentOne(EsInsertData esInsertData);

    /**
     * 插入一条微博文章内容数据
     * @param esInsertData
     * @return
     */
    int insertWbArticleContentOne(EsInsertData esInsertData);

    /**
     * 批量插入微信文章内容数据
     * @param esInsertDatas
     * @return
     */
    int insertWxArticleContentBatch(List<EsInsertData> esInsertDatas);

    /**
     * 批量插入微博文章内容数据
     * @param esInsertDatas
     * @return
     */
    int insertWbArticleContentBatch(List<EsInsertData> esInsertDatas);

    /**
     * 更新并批量插入微信文章标题数据
     * @param esInsertDatas
     * @return
     */
    int upsertWxArticleTitleBatch(List<EsInsertData> esInsertDatas);

    /**
     * 更新并批量插入微信文章内容数据
     * @param esInsertDatas
     * @return
     */
    int upsertWxArticleContentBatch(List<EsInsertData> esInsertDatas);

    /**
     * 更新并批量插入微博文章标题数据
     * @param esInsertDatas
     * @return
     */
    int upsertWbArticleTitleBatch(List<EsInsertData> esInsertDatas);

    /**
     * 更新并批量插入微博文章内容数据
     * @param esInsertDatas
     * @return
     */
    int upsertWbArticleContentBatch(List<EsInsertData> esInsertDatas);

    /**
     * 从mongo索引微信文章标题
     * @param esInsertData
     * @return
     */
    int insertWxArticleTitleFromMongo(EsInsertData esInsertData);

    /**
     * 从mongo索引微信文章内容
     * @param esInsertData
     * @return
     */
    int insertWxArticleContentFromMongo(EsInsertData esInsertData);

    /**
     * 从mongo索引微博文章标题
     * @param esInsertData
     * @return
     */
    int insertWbArticleTitleFromMongo(EsInsertData esInsertData);

    /**
     * 从mongo索引微博文章内容
     * @param esInsertData
     * @return
     */
    int insertWbArticleContentFromMongo(EsInsertData esInsertData);

    /**
     * 从mongo索引微信媒体到ES
     * @param esInsertData
     * @return
     */
    int insertWxMediaFromMongo(EsInsertData esInsertData);

    /**
     * 从mongo索引微博媒体到ES
     * @param esInsertData
     * @return
     */
    int insertWbMediaFromMongo(EsInsertData esInsertData);

    /**
     * 从mongo更新或索引微信媒体
     * @param esInsertData
     * @return
     */
    int upsertWxMediaFromMongo(EsInsertData esInsertData);

    /**
     * 从mongo更新或索引微博媒体
     * @param esInsertData
     * @return
     */
    int upsertWbMediaFromMongo(EsInsertData esInsertData);

    /**
     * 从mongo导入ES直播媒体数据
     * @param esInsertData
     * @return
     */
    int insertLiveMediaFromMongo(EsInsertData esInsertData);

    /**
     * 从kafka导入ES直播媒体数据
     * @param esInsertData
     * @return
     */
    int insertLiveMediaFromKafka(EsInsertData esInsertData);

    int insertWebSearchMedia(ReqMediaBasicIndex reqMediaBasicIndex);
    /**
     * 手动刷新bulk
     */
    void flushEsBulk();
}
