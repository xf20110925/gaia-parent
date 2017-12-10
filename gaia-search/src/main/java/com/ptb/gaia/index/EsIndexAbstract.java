package com.ptb.gaia.index;

import com.ptb.gaia.index.bean.EsInsertData;
import com.ptb.gaia.index.bean.ReqMediaBasicIndex;
import com.ptb.gaia.index.es.IndexServerEs;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.List;

/**
 * Created by watson zhang on 2017/3/20.
 */
public abstract class EsIndexAbstract implements IIndex{
    Configuration conf;
    public static String mediaIndex;
    public static String wxMediaType;
    public static String wbMediaType;
    public static String liveMediaType;
    public static String articleTitleIndex;
    public static String articleContentIndex;
    public static String wxArticleType;
    public static String wbArticleType;

    IndexServerEs indexServerEs;
    public EsIndexAbstract(){
        try {
            conf = new PropertiesConfiguration("ptb.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        mediaIndex = conf.getString("gaia.es.media.index","media");
        articleTitleIndex = conf.getString("gaia.es.article.title.index","article_m");
        articleContentIndex = conf.getString("gaia.es.article.content.index","article_content");
        wxMediaType = conf.getString("gaia.es.wxmedia.type", "weixinMedia");
        wbMediaType = conf.getString("gaia.es.wbmedia.type", "weiboMedia");
        liveMediaType = conf.getString("gaia.es.livemedia.type", "liveMedia");
        wxArticleType = conf.getString("gaia.es.wxArticle.type", "wxArticle");
        wbArticleType = conf.getString("gaia.es.wbArticle.type", "wbArticle");

        indexServerEs = new IndexServerEs();
    }


    /**
     * 插入一条微信媒体数据
     * @param esInsertData
     * @return
     */
    public int insertWxMediaOne(EsInsertData esInsertData){return 0;}

    /**
     * 插入一条微博媒体数据
     * @param esInsertData
     * @return
     */
    public int insertWbMediaOne(EsInsertData esInsertData){return 0;}

    /**
     * 批量插入微信媒体数据
     * @param esInsertData
     * @return
     */
    public int insertWxMediaBatch(List<EsInsertData> esInsertData){return 0;}

    /**
     * 批量插入微博媒体数据
     * @param esInsertData
     * @return
     */
    public int insertWbMediaBatch(List<EsInsertData> esInsertData){return 0;}

    /**
     * 获取缓存中的数据条数
     * @return
     */
    public int getBulkNum(){return 0;}

    /**
     * 更新并插入一条微信数据
     * @param esInsertData
     * @return
     */
    public int upsertWxMediaOne(EsInsertData esInsertData){return 0;}

    /**
     * 更新并插入一条微博媒体数据
     * @param esInsertData
     * @return
     */
    public int upsertWbMediaOne(EsInsertData esInsertData){return 0;}

    /**
     * 更新并批量插入微信媒体数据
     * @param esInsertDatas
     * @return
     */
    public int upsertWxMediaBatch(List<EsInsertData> esInsertDatas){return 0;}

    /**
     * 更新并批量插入微博媒体数据
     * @param esInsertDatas
     * @return
     */
    public int upsertWbMediaBatch(List<EsInsertData> esInsertDatas){return 0;}

    /**
     * 插入一条文章标题数据
     * @param esInsertData
     * @return
     */
    public int insertWxArticleTitleOne(EsInsertData esInsertData){return 0;}

    /**
     * 插入一条微博文章标题数据
     * @param esInsertData
     * @return
     */
    public int insertWbArticleTitleOne(EsInsertData esInsertData){return 0;}

    /**
     * 批量插入微信文章标题数据
     * @param esInsertData
     * @return
     */
    public int insertWxArticleTitleBatch(List<EsInsertData> esInsertData){return 0;}

    /**
     * 批量插入微博文章标题数据
     * @param esInsertData
     * @return
     */
    public int insertWbArticleTitleBatch(List<EsInsertData> esInsertData){return 0;}

    /**
     * 插入一条微信文章内容数据
     * @param esInsertData
     * @return
     */
    public int insertWxArticleContentOne(EsInsertData esInsertData){return 0;}

    /**
     * 插入一条微博文章内容数据
     * @param esInsertData
     * @return
     */
    public int insertWbArticleContentOne(EsInsertData esInsertData){return 0;}

    /**
     * 批量插入微信文章内容数据
     * @param esInsertDatas
     * @return
     */
    public int insertWxArticleContentBatch(List<EsInsertData> esInsertDatas){return 0;}

    /**
     * 批量插入微博文章内容数据
     * @param esInsertDatas
     * @return
     */
    public int insertWbArticleContentBatch(List<EsInsertData> esInsertDatas){return 0;}

    /**
     * 更新并批量插入微信文章标题数据
     * @param esInsertDatas
     * @return
     */
    public int upsertWxArticleTitleBatch(List<EsInsertData> esInsertDatas){return 0;}

    /**
     * 更新并批量插入微信文章内容数据
     * @param esInsertDatas
     * @return
     */
    public int upsertWxArticleContentBatch(List<EsInsertData> esInsertDatas){return 0;}

    /**
     * 更新并批量插入微博文章标题数据
     * @param esInsertDatas
     * @return
     */
    public int upsertWbArticleTitleBatch(List<EsInsertData> esInsertDatas){return 0;}

    /**
     * 更新并批量插入微博文章内容数据
     * @param esInsertDatas
     * @return
     */
    public int upsertWbArticleContentBatch(List<EsInsertData> esInsertDatas){return 0;}

    /**
     * 从mongo索引微信文章标题
     * @param esInsertData
     * @return
     */
    public int insertWxArticleTitleFromMongo(EsInsertData esInsertData){return 0;}

    /**
     * 从mongo索引微信文章内容
     * @param esInsertData
     * @return
     */
    public int insertWxArticleContentFromMongo(EsInsertData esInsertData){return 0;}

    /**
     * 从mongo索引微博文章标题
     * @param esInsertData
     * @return
     */
    public int insertWbArticleTitleFromMongo(EsInsertData esInsertData){return 0;}

    /**
     * 从mongo索引微博文章内容
     * @param esInsertData
     * @return
     */
    public int insertWbArticleContentFromMongo(EsInsertData esInsertData){return 0;}

    /**
     * 从mongo索引微信媒体到ES
     * @param esInsertData
     * @return
     */
    public int insertWxMediaFromMongo(EsInsertData esInsertData){return 0;}

    /**
     * 从mongo索引微博媒体到ES
     * @param esInsertData
     * @return
     */
    public int insertWbMediaFromMongo(EsInsertData esInsertData){return 0;}

    /**
     * 从mongo更新或索引微信媒体
     * @param esInsertData
     * @return
     */
    public int upsertWxMediaFromMongo(EsInsertData esInsertData){return 0;}

    /**
     * 从mongo更新或索引微博媒体
     * @param esInsertData
     * @return
     */
    public int upsertWbMediaFromMongo(EsInsertData esInsertData){return 0;}

    /**
     * 从mongo导入ES直播媒体数据
     * @param esInsertData
     * @return
     */
    public int insertLiveMediaFromMongo(EsInsertData esInsertData){return 0;}


    /**
     * 从kafka导入ES直播媒体数据
     * @param esInsertData
     * @return
     */
    public int insertLiveMediaFromKafka(EsInsertData esInsertData){return 0;}

    public int insertWebSearchMedia(ReqMediaBasicIndex reqMediaBasicIndex){return 0;}
    /**
     * 手动刷新bulk
     */
    public void flushEsBulk(){}
}
