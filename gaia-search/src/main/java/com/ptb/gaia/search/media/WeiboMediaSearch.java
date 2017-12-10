package com.ptb.gaia.search.media;

import com.ptb.gaia.index.bean.WbReqMediaSearch;
import com.ptb.gaia.search.utils.EsRspResult;
import com.ptb.gaia.service.entity.media.GWbMedia;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.lucene.queryparser.xml.FilterBuilder;
import org.apache.lucene.queryparser.xml.FilterBuilderFactory;
import org.apache.lucene.queryparser.xml.builders.RangeFilterBuilder;
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

/**
 * Created by watson zhang on 16/7/18.
 */
public class WeiboMediaSearch {

    private static final Logger logger = LoggerFactory.getLogger(WeiboMediaSearch.class);
    String WBESTYPE = "weiboMedia";
    MediaSearchHandle handle;

    public WeiboMediaSearch(){
        this.handle = MediaSearchHandle.getInstance();
    }

    private SortBuilder getTagsSort(){
        String scriptWeibo = "if (doc['tags'].value != null) { authI = 1000.0}else{ authI = 0.0 };" +
                "_score*doc['mediaScore'].value + authI";
        Script script = new Script(scriptWeibo);
        SortBuilder sortBuilder = SortBuilders.scriptSort(script, "number").order(SortOrder.DESC);
        return sortBuilder;
    }

    public List<GWbMedia> searchWeiboMediaTags(String mediaName, int startPos, int endPos){
        try {
            QueryBuilder qb = QueryBuilders.boolQuery()
                    .should(matchQuery("mediaName", mediaName))
                    .should(matchPhraseQuery("tags", mediaName)).boost(10f);
            SortBuilder sort = this.getTagsSort();
            List<String> wbIdList = handle.searchMediaHandle(qb, sort, startPos, endPos-startPos, WBESTYPE);
            return handle.getWeiboMediaInfo(wbIdList);
        } catch (Exception e){
            logger.error("mediaName: {}, startPos: {}, endPos: {}", mediaName, startPos, endPos);
            return null;
        }
    }

    private SortBuilder getSort(){
        String scriptWeibo = "_score*doc['mediaScore'].value";
        Script script = new Script(scriptWeibo);
        SortBuilder sortBuilder = SortBuilders.scriptSort(script, "number").order(SortOrder.DESC);
        return sortBuilder;
    }

    public List<GWbMedia> searchWeiboMediaInfo(String mediaName, int startPos, int endPos) {
        try {
            QueryBuilder qb = matchQuery("mediaName", mediaName);
            SortBuilder sort = this.getSort();
            List<String> wbIdList = handle.searchMediaHandle(qb, sort, startPos, endPos-startPos, WBESTYPE);
            return handle.getWeiboMediaInfo(wbIdList);
        } catch (Exception e){
            logger.error("mediaName: {}, startPos: {}, endPos: {}", mediaName, startPos, endPos);
            return null;
        }
    }

    @Deprecated
    public EsRspResult<GWbMedia> searchWeiboMedia(int sType, String mediaName, int start, int end){
        if(start >= end || mediaName == null){
            return null;
        }

        QueryBuilder qb;
        SortBuilder sort;
        if (sType == 1){
            //APP查询接口
            qb = matchQuery("mediaName", mediaName);
            sort = this.getSort();
        }else if (sType == 2){
            //公众号查询接口
            qb = QueryBuilders.boolQuery()
                    .should(matchQuery("mediaName", mediaName))
                    .should(matchPhraseQuery("tags", mediaName)).boost(10f);
            sort = this.getTagsSort();
        }else {
            return null;
        }
        return this.searchWeiboInfo(qb, sort, start, end);
    }

    public EsRspResult<GWbMedia> searchWeiboMedia(String mediaName, int start, int end){
        if(start >= end || mediaName == null){
            return null;
        }
        QueryBuilder qb;
        SortBuilder sort;
        //APP查询接口
        qb = matchQuery("mediaName", mediaName);
        sort = this.getSort();
        return this.searchWeiboInfo(qb, sort, start, end);
    }

    public EsRspResult<GWbMedia> searchWeiboInfo(QueryBuilder qb, SortBuilder sort, int start, int end) {
        if (qb == null || sort == null){
            return null;
        }
        try {
            ImmutablePair<Long, List<String>> wbIdList = handle.searchMediaTotalHandle(qb, sort, start, end-start, WBESTYPE);
            if (wbIdList == null){
                return null;
            }
            EsRspResult<GWbMedia> esRspResult = new EsRspResult<>();
            esRspResult.setTotalNum(wbIdList.getLeft());
            esRspResult.setMediaList(handle.getWeiboMediaInfo(wbIdList.getRight()));
            return esRspResult;
        } catch (Exception e){
            logger.error("start: {}, end: {}", start, end);
            return null;
        }
    }


    public EsRspResult<String> searchWbInfoRorWeb(WbReqMediaSearch wbReqMediaSearch){
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        if (wbReqMediaSearch.getKey() != null && wbReqMediaSearch.getKey().length() > 0){
            MatchQueryBuilder query = QueryBuilders.matchQuery("mediaName", wbReqMediaSearch.getKey());
            boolQuery.must(query);
        }

        boolean isHave = false;

        RangeQueryBuilder priceQuery = QueryBuilders.rangeQuery("price");
        if (wbReqMediaSearch.getMinPrice() > 0){
            priceQuery.gte(wbReqMediaSearch.getMinPrice()*100);
            isHave = true;
        }
        if (wbReqMediaSearch.getMaxPrice() > 0){
            priceQuery.lte(wbReqMediaSearch.getMaxPrice()*100);
            isHave = true;
        }
        if (isHave == true){
            boolQuery.filter(priceQuery);
            isHave = false;
        }

        if (wbReqMediaSearch.getClassify() != 0){
            TermQueryBuilder classifyQuery = QueryBuilders.termQuery("iId", wbReqMediaSearch.getClassify());
            boolQuery.filter(classifyQuery);
        }

        RangeQueryBuilder fansQuery = QueryBuilders.rangeQuery("fansNum");
        if (wbReqMediaSearch.getMinFans() > 0){
            fansQuery.gte(wbReqMediaSearch.getMinFans());
            isHave = true;
        }
        if (wbReqMediaSearch.getMaxFans() > 0){
            fansQuery.lte(wbReqMediaSearch.getMaxFans());
            isHave = true;
        }
        if (isHave == true){
            boolQuery.filter(fansQuery);
            isHave = false;
        }

        RangeQueryBuilder forwardQuery = QueryBuilders.rangeQuery("avgSpread");
        if (wbReqMediaSearch.getMinForward() > 0){
            forwardQuery.gte(wbReqMediaSearch.getMinForward());
            isHave = true;
        }
        if (wbReqMediaSearch.getMaxForward() > 0){
            forwardQuery.lte(wbReqMediaSearch.getMaxForward());
            isHave = true;
        }
        if (isHave == true){
            boolQuery.filter(forwardQuery);
            isHave = false;
        }

        RangeQueryBuilder commentQuery = QueryBuilders.rangeQuery("avgComment");
        if (wbReqMediaSearch.getMinComment() > 0){
            commentQuery.gte(wbReqMediaSearch.getMinComment());
            isHave = true;
        }
        if (wbReqMediaSearch.getMaxComment() > 0){
            commentQuery.lte(wbReqMediaSearch.getMaxComment());
            isHave = true;
        }
        if (isHave == true){
            boolQuery.filter(commentQuery);
            isHave = false;
        }

        RangeQueryBuilder likeQuery = QueryBuilders.rangeQuery("avgLike");
        if (wbReqMediaSearch.getMinLike() > 0){
            likeQuery.gte(wbReqMediaSearch.getMinLike());
            isHave = true;
        }
        if (wbReqMediaSearch.getMaxLike() > 0){
            likeQuery.lte(wbReqMediaSearch.getMaxLike());
            isHave = true;
        }
        if (isHave == true){
            boolQuery.filter(likeQuery);
            isHave = false;
        }

        if (wbReqMediaSearch.getIsAuth() == 1){
            boolQuery.filter(QueryBuilders.termQuery("isAuth", 1));
        }else if (wbReqMediaSearch.getIsAuth() == 2)
            boolQuery.filter(QueryBuilders.termQuery("isAuth", 0));

        if (wbReqMediaSearch.getSeller() == 1){
            boolQuery.filter(QueryBuilders.rangeQuery("agentNum").gt(0));
        }else if (wbReqMediaSearch.getSeller() == 2){
            boolQuery.filter(QueryBuilders.rangeQuery("agentNum").lte(0));
        }

        FieldSortBuilder sortBuilder = null;
        if (wbReqMediaSearch.getSort() == 1){
            sortBuilder = null;
        } else if (wbReqMediaSearch.getSort() == 2){
            sortBuilder = SortBuilders.fieldSort("fansNum").order(SortOrder.DESC);
        } else if (wbReqMediaSearch.getSort() == 3){
            sortBuilder = SortBuilders.fieldSort("avgSpread").order(SortOrder.DESC);
        } else if (wbReqMediaSearch.getSort() == 4){
            sortBuilder = SortBuilders.fieldSort("avgComment").order(SortOrder.DESC);
        } else if (wbReqMediaSearch.getSort() == 5){
            sortBuilder = SortBuilders.fieldSort("avgLike").order(SortOrder.DESC);
        } else if (wbReqMediaSearch.getSort() == 6){
            sortBuilder = SortBuilders.fieldSort("agentNum").order(SortOrder.DESC);
        }else if (wbReqMediaSearch.getSort() == 7){
            sortBuilder = SortBuilders.fieldSort("price").order(SortOrder.DESC);
        }
        ImmutablePair<Long, List<String>> longListImmutablePair = handle.searchMediaTotalHandle(boolQuery,
                sortBuilder,
                wbReqMediaSearch.getStart(),
                wbReqMediaSearch.getEnd() - wbReqMediaSearch.getStart(),
                WBESTYPE);
        if (longListImmutablePair == null){
            return null;
        }

        EsRspResult esRspResult = new EsRspResult();
        esRspResult.setTotalNum(longListImmutablePair.getLeft());
        esRspResult.setMediaList(longListImmutablePair.getRight());
        return esRspResult;

    }
}
