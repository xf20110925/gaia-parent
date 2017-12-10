package com.ptb.gaia.search.article;

import com.ptb.gaia.search.bean.PostTime;
import com.ptb.gaia.search.bean.ReadNum;
import com.ptb.gaia.search.bean.ReqArticleBase;
import com.ptb.gaia.service.entity.article.GArticleBasic;
import com.ptb.gaia.service.entity.article.GWbArticle;
import com.ptb.gaia.service.entity.article.GWxArticle;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.index.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.ptb.gaia.search.article.ArticleSearchEsImpl.TWbArticleType;
import static com.ptb.gaia.search.article.ArticleSearchEsImpl.TWxArticleType;

/**
 * Created by watson zhang on 2017/3/13.
 */
public class ContentSearch {
    private static Logger logger = LoggerFactory.getLogger(ContentSearch.class);
    ArticleSearchEsImpl articleSearchEsImpl;

    public ContentSearch(){
        articleSearchEsImpl = new ArticleSearchEsImpl();
    }

    /**
     * 搜索文章内容
     * @param reqArticleBase
     * @param readNum
     * @param postTime
     * @return
     */
    public <T extends  GArticleBasic> Pair<Long, List<T>> searchArticleContent(ReqArticleBase reqArticleBase, ReadNum readNum, PostTime postTime){
        if (reqArticleBase == null || readNum == null || postTime == null){
            logger.error("title search error! reqArticleBase: {}\treadNum: {}\tpostTime: {}", reqArticleBase, readNum, postTime);
            return null;
        }

        MatchQueryBuilder content = QueryBuilders.matchQuery("content", reqArticleBase.getKeyword());
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(content);
        RangeQueryBuilder readNumQuery = QueryBuilders.rangeQuery("readNum");
        if (readNum.getStart() > 0){
            readNumQuery.gte(readNum.getStart());
        }
        if (readNum.getEnd() > 0){
            readNumQuery.lte(readNum.getEnd());
        }

        queryBuilder.must(readNumQuery);
        RangeQueryBuilder postTimeQuery = QueryBuilders.rangeQuery("postTime");
        if (postTime.getStart() > 0){
            postTimeQuery.gte(postTime.getStart());
        }
        if (postTime.getEnd() > 0){
            postTimeQuery.lte(postTime.getEnd());
        }

        queryBuilder.must(postTimeQuery);

        String type = null;
        Class tClass = null;
        if (reqArticleBase.getMediaType() == 1) {
            type = TWxArticleType;
            tClass = GWxArticle.class;
        } else if (reqArticleBase.getMediaType() == 2) {
            type = TWbArticleType;
            tClass = GWbArticle.class;
        }
        Pair<Long, List<T>> listPairExact = articleSearchEsImpl.searchArticleContent(
                tClass,
                reqArticleBase.getStart(),
                reqArticleBase.getEnd(),
                type,
                queryBuilder,
                null,
                null,
                null);
        return listPairExact;
    }

}
