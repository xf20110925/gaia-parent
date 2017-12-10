package com.ptb.gaia.search;

import com.ptb.gaia.index.bean.ReqMediaBasicSearch;
import com.ptb.gaia.search.bean.PostTime;
import com.ptb.gaia.search.bean.ReadNum;
import com.ptb.gaia.search.bean.ReqArticleBase;
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
public interface ISearch {
    @Deprecated
    <T extends GMediaBasic> List<T> searchMediaInfo(String mediaName, int startPos, int endPos, Class<T> tClass);

    EsRspResult<GWxMedia> searchWxMedia(String mediaName, int startPos, int endPos);

    EsRspResult<GWbMedia> searchWbMedia(String mediaName, int startPos, int endPos);

    EsRspResult<String> searchMediaForWeb(ReqMediaBasicSearch reqMediaBasicSearch);

    @Deprecated
    <T extends GArticleBasic> List<T> searchArticle(
            String keyword, long time, Class<T> tClass, int offset, int size);

    @Deprecated
    <T extends GArticleBasic> List<T> searchArticle(
            String keyword, Class<T> tClass, int offset, int size);

    @Deprecated
    <T extends GArticleBasic> Pair<Long, List<T>> searchArticle(String keyword, int sortType, long time, Class<T> tClass, int offset, int size);

    <T extends GArticleBasic> List<T> contentSearchByPmid(
            String pmid, Class<T> tClass, int offset, int size);

    @Deprecated
    <T extends GArticleBasic> Pair<Long, List<T>> searchArticleContent(String keyword, int sortType, long time, Class<T> tClass, int start, int end);

    /**
     * @Description: According to urls return List[Content] wx or wb
     * @param: Set<String> urls,Class<T> tClass
     * @return: List[Content]
     * @author: mengchen
     * @Date: 2017/2/14
     */
    <T extends GArticleBasic> List<String> searchArticleContentByUrl(Set<String> urls, Class<T> tClass);

    /**
     * 搜索微博和微信的文章标题和内容功能，全都用着一个接口就可以，其他接口不用了，add by watson。
     * @param reqArticleBase
     * @param readNum
     * @param postTime
     * @param <T>
     * @return
     */
    <T extends GArticleBasic> Pair<Long, List<T>> searchArticle(ReqArticleBase reqArticleBase, ReadNum readNum, PostTime postTime);



}
