package com.ptb.gaia.search.article;

import com.ptb.gaia.search.bean.PostTime;
import com.ptb.gaia.search.bean.ReadNum;
import com.ptb.gaia.search.bean.ReqArticleBase;
import com.ptb.gaia.service.entity.article.GArticleBasic;
import com.ptb.gaia.service.entity.article.GWbArticle;
import com.ptb.gaia.service.entity.article.GWxArticle;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.ptb.gaia.search.article.ArticleSearchEsImpl.TWbArticleType;
import static com.ptb.gaia.search.article.ArticleSearchEsImpl.TWxArticleType;

/**
 * Created by watson zhang on 2017/3/13.
 */
public class TitleSearch {
    private static Logger logger = LoggerFactory.getLogger(TitleSearch.class);
    ArticleSearchEsImpl articleSearchEsImpl;

    public TitleSearch(){
        articleSearchEsImpl = new ArticleSearchEsImpl();
    }

    /**
     * 搜索微信文章标题
     * @param reqArticleBase
     * @param readNum
     * @param postTime
     */
    public <T extends GArticleBasic> Pair<Long, List<T>> searchArticleTitle(ReqArticleBase reqArticleBase, ReadNum readNum, PostTime postTime){
        if (reqArticleBase == null || readNum == null || postTime == null){
            logger.error("title search error! reqArticleBase: {}\treadNum: {}\tpostTime: {}", reqArticleBase, readNum, postTime);
            return null;
        }

        MatchQueryBuilder content = QueryBuilders.matchQuery("title", reqArticleBase.getKeyword());
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(content);

        if(readNum.getStart() > 0 || readNum.getEnd() > 0){
            RangeQueryBuilder readNumQuery = null;
            if (reqArticleBase.getMediaType() == 1){
                readNumQuery = QueryBuilders.rangeQuery("readNum");
            }else if (reqArticleBase.getMediaType() == 2){
                readNumQuery = QueryBuilders.rangeQuery("zpzNum");
            }
            if (readNum.getStart() > 0){
                readNumQuery.gte(readNum.getStart());
            }
            if (readNum.getEnd() > 0){
                readNumQuery.lte(readNum.getEnd());
            }

            queryBuilder.must(readNumQuery);
        }

        if ((postTime.getStart() > 0) || (postTime.getStart() > 0)){
            RangeQueryBuilder postTimeQuery = QueryBuilders.rangeQuery("postTime");
            if (postTime.getStart() > 0){
                postTimeQuery.gte(postTime.getStart());
            }
            if (postTime.getEnd() > 0){
                postTimeQuery.lte(postTime.getEnd());
            }

            queryBuilder.must(postTimeQuery);
        }

        String type = null;
        Class tClass = null;
        if (reqArticleBase.getMediaType() == 1) {
            type = TWxArticleType;
            tClass = GWxArticle.class;
        } else if (reqArticleBase.getMediaType() == 2) {
            type = TWbArticleType;
            tClass = GWbArticle.class;
        }
        Pair<Long, List<T>> listPairExact = articleSearchEsImpl.searchArticleTitle(
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
