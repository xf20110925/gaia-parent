package com.ptb.gaia.search;

import com.ptb.gaia.index.bean.LiveReqMediaSearch;
import com.ptb.gaia.index.bean.ReqMediaBasicSearch;
import com.ptb.gaia.index.bean.WbReqMediaSearch;
import com.ptb.gaia.index.bean.WxReqMediaSearch;
import com.ptb.gaia.search.article.ArticleSearchEsImpl;
import com.ptb.gaia.search.article.ContentSearch;
import com.ptb.gaia.search.article.IArticleSearch;
import com.ptb.gaia.search.article.TitleSearch;
import com.ptb.gaia.search.bean.PostTime;
import com.ptb.gaia.search.bean.ReadNum;
import com.ptb.gaia.search.bean.ReqArticleBase;
import com.ptb.gaia.search.media.LiveMediaSearch;
import com.ptb.gaia.search.media.WeiboMediaSearch;
import com.ptb.gaia.search.media.WeixinMediaSearch;
import com.ptb.gaia.search.utils.EsRspResult;
import com.ptb.gaia.service.entity.article.GArticleBasic;
import com.ptb.gaia.service.entity.media.GMediaBasic;
import com.ptb.gaia.service.entity.media.GWbMedia;
import com.ptb.gaia.service.entity.media.GWxMedia;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * Created by eric on 16/8/10.
 */
public class ISearchImpl implements ISearch {
    WeiboMediaSearch weiboMediaSearch = new WeiboMediaSearch();
    WeixinMediaSearch weixinMediaSearch = new WeixinMediaSearch();
    LiveMediaSearch liveMediaSearch = new LiveMediaSearch();
    IArticleSearch iArticleSearch = new ArticleSearchEsImpl();

    ContentSearch contentSearch = new ContentSearch();
    TitleSearch titleSearch = new TitleSearch();

    @Override
    public <T extends GMediaBasic> List<T> searchMediaInfo(
            String mediaName, int startPos, int endPos, Class<T> tClass) {
        if (tClass == GWxMedia.class) {
            return (List<T>) weixinMediaSearch.searchWeixinMediaInfo(mediaName, startPos, endPos);
        } else {
            return (List<T>) weiboMediaSearch.searchWeiboMediaInfo(mediaName, startPos, endPos);
        }
    }

    @Override
    public EsRspResult<GWxMedia> searchWxMedia(String mediaName, int startPos, int endPos) {
        if (mediaName == null || startPos >= endPos){
            return null;
        }
        EsRspResult<GWxMedia> gWxMediaEsRspResult = weixinMediaSearch.searchWeixinMedia(mediaName, startPos, endPos);

        return gWxMediaEsRspResult;
    }

    @Override
    public EsRspResult<GWbMedia> searchWbMedia(String mediaName, int startPos, int endPos) {
        if (mediaName == null || startPos >= endPos){
            return null;
        }
        EsRspResult<GWbMedia> gWbMediaEsRspResult = weiboMediaSearch.searchWeiboMedia(mediaName, startPos, endPos);
        return gWbMediaEsRspResult;
    }

    @Override
    public EsRspResult<String> searchMediaForWeb(ReqMediaBasicSearch reqMediaBasicSearch) {
        EsRspResult<String> esRspResult = null;
        if (reqMediaBasicSearch.getMediaType() == 1){
            esRspResult = weixinMediaSearch.searchWxInfoForWeb((WxReqMediaSearch) reqMediaBasicSearch);
        }else if (reqMediaBasicSearch.getMediaType() == 2){
            esRspResult = weiboMediaSearch.searchWbInfoRorWeb((WbReqMediaSearch) reqMediaBasicSearch);
        }else if (reqMediaBasicSearch.getMediaType() >= 3 && reqMediaBasicSearch.getMediaType() <= 6){
            esRspResult = liveMediaSearch.searchLiveInfoForWeb((LiveReqMediaSearch) reqMediaBasicSearch);
        }
        return esRspResult;
    }

    @Override
    public <T extends GArticleBasic> List<T> searchArticle(
            String keyword, long time, Class<T> tClass, int offset,
            int size) {
        return iArticleSearch.search(keyword, time, tClass, offset, size);
    }

    @Override
    public <T extends GArticleBasic> List<T> searchArticle(
            String keyword, Class<T> tClass, int offset, int size) {
        return iArticleSearch.search(keyword, tClass, offset, size);
    }

    @Override
    public <T extends GArticleBasic> List<T> contentSearchByPmid(String pmid, Class<T> tClass, int offset, int size) {
        return iArticleSearch.contentSearchByPmid(pmid, tClass, offset, size);
    }

    @Override
    public <T extends GArticleBasic> Pair<Long, List<T>> searchArticle(String keyword, int sortType, long time, Class<T> tClass, int offset, int size) {
        return iArticleSearch.searchWbArticle(keyword, sortType, time, tClass, offset, size);
    }

    @Override
    public <T extends GArticleBasic> Pair<Long, List<T>> searchArticleContent(String keyword, int sortType, long time, Class<T> tClass, int start, int end) {
        return iArticleSearch.searchArticleContent(keyword, sortType, tClass, start, end);
    }

    @Override
    public <T extends GArticleBasic> List<String> searchArticleContentByUrl(Set<String> urls, Class<T> tClass) {
        return iArticleSearch.searchArticleContentByUrl(urls,tClass);
    }

    @Override
    public <T extends GArticleBasic> Pair<Long, List<T>> searchArticle(ReqArticleBase reqArticleBase, ReadNum readNum, PostTime postTime) {
        Pair<Long, List<T>> longListPair = null;
        if (reqArticleBase.getSearchType() == 1){
            longListPair = titleSearch.searchArticleTitle(reqArticleBase, readNum, postTime);
        }else if (reqArticleBase.getSearchType() == 2){
            longListPair = contentSearch.searchArticleContent(reqArticleBase, readNum, postTime);
        }
        return longListPair;
    }

}
