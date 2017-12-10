package com.ptb.gaia.search.media;

import com.ptb.gaia.index.bean.LiveReqMediaSearch;
import com.ptb.gaia.search.utils.EsRspResult;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

/**
 * Created by watson zhang on 2017/3/21.
 */
public class LiveMediaSearch {
    private static final Logger logger = LoggerFactory.getLogger(WeiboMediaSearch.class);
    String LIVEESTYPE = "liveMedia";
    MediaSearchHandle handle;

    public LiveMediaSearch(){
        this.handle = MediaSearchHandle.getInstance();
    }

    private SortBuilder getSort(String mediaName){
        String scriptWeixin = "return _score";
        Script script = new Script(scriptWeixin);
        return SortBuilders.scriptSort(script, "number").order(SortOrder.DESC).missing("_last");
    }

    public EsRspResult<String> searchLiveInfo(String mediaName, int start, int end){
        if (mediaName == null || mediaName.length() <= 0){
            return null;
        }
        QueryBuilder qb = matchQuery("mediaName", mediaName);
        SortBuilder sort = this.getSort(mediaName);
        EsRspResult<String> esRspResult = new EsRspResult<>();
        ImmutablePair<Long, List<String>> wxIdList = handle.searchMediaTotalHandle(qb, sort, start, end-start, LIVEESTYPE);
        if (wxIdList == null){
            return null;
        }
        esRspResult.setTotalNum(wxIdList.getLeft());
        esRspResult.setMediaList(wxIdList.getRight());

        return esRspResult;
    }

    /**
     * 搜索直播媒体
     * @param liveReqMediaSearch
     * @return
     */
    public EsRspResult<String> searchLiveInfoForWeb(LiveReqMediaSearch liveReqMediaSearch){
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        if (liveReqMediaSearch.getKey() != null && liveReqMediaSearch.getKey().length() > 0){
            MatchQueryBuilder query = QueryBuilders.matchQuery("mediaName", liveReqMediaSearch.getKey());
            boolQuery.must(query);
        }
        boolean isHave = false;

        RangeQueryBuilder priceQuery = QueryBuilders.rangeQuery("price");
        if (liveReqMediaSearch.getMinPrice() > 0){
            priceQuery.gte(liveReqMediaSearch.getMinPrice()*100);
            isHave = true;
        }
        if (liveReqMediaSearch.getMaxPrice() > 0){
            priceQuery.lte(liveReqMediaSearch.getMaxPrice()*100);
            isHave = true;
        }
        if (isHave == true){
            boolQuery.filter(priceQuery);
            isHave = false;
        }

        RangeQueryBuilder fansQuery = QueryBuilders.rangeQuery("fansNum");
        if (liveReqMediaSearch.getMinFans() > 0){
            fansQuery.gte(liveReqMediaSearch.getMinFans());
            isHave = true;
        }
        if (liveReqMediaSearch.getMaxFans() > 0){
            fansQuery.lte(liveReqMediaSearch.getMaxFans());
            isHave = true;
        }
        if (isHave == true){
            boolQuery.filter(fansQuery);
            isHave = false;
        }

        RangeQueryBuilder onlineNumQuery = QueryBuilders.rangeQuery("avgOnlineNum");
        if (liveReqMediaSearch.getMinOnline() > 0){
            onlineNumQuery.gte(liveReqMediaSearch.getMinOnline());
            isHave = true;
        }
        if (liveReqMediaSearch.getMaxOnline() > 0){
            onlineNumQuery.lte(liveReqMediaSearch.getMaxOnline());
            isHave = true;
        }
        if (isHave == true){
            boolQuery.filter(onlineNumQuery);
            isHave = false;
        }

        if (liveReqMediaSearch.getGender() == 1){
            boolQuery.filter(QueryBuilders.termQuery("gender", 1));
        }else if (liveReqMediaSearch.getGender() == 2){
            boolQuery.filter(QueryBuilders.termQuery("gender", 2));
        }else if (liveReqMediaSearch.getGender() == 3){
            boolQuery.filter(QueryBuilders.termQuery("gender", 3));
        }

        if (liveReqMediaSearch.getIsAuth() == 1){
            boolQuery.filter(QueryBuilders.termQuery("isAuth", 1));
        }else if (liveReqMediaSearch.getIsAuth() == 2){
            boolQuery.filter(QueryBuilders.termQuery("isAuth", 0));
        }

        if (liveReqMediaSearch.getSeller() == 1){
            boolQuery.filter(QueryBuilders.rangeQuery("agentNum").gte(1));
        }else if (liveReqMediaSearch.getSeller() == 2){
            boolQuery.filter(QueryBuilders.rangeQuery("agentNum").lte(0));
        }

        if (liveReqMediaSearch.getLocation() != null && liveReqMediaSearch.getLocation().length() >= 0){
            boolQuery.filter(QueryBuilders.termQuery("location", liveReqMediaSearch.getLocation()));
        }


        FieldSortBuilder sortBuilder = null;
        if (liveReqMediaSearch.getSort() == 1){
            sortBuilder = null;
        }else if (liveReqMediaSearch.getSort() == 2){
            sortBuilder = SortBuilders.fieldSort("fansNum").order(SortOrder.DESC);
        }else if (liveReqMediaSearch.getSort() == 3){
            sortBuilder = SortBuilders.fieldSort("avgOnlineNum").order(SortOrder.DESC);
        }else if (liveReqMediaSearch.getSort() == 4){
            sortBuilder = SortBuilders.fieldSort("agentNum").order(SortOrder.DESC);
        }else if (liveReqMediaSearch.getSort() == 5){
            sortBuilder = SortBuilders.fieldSort("price").order(SortOrder.DESC);
        }
        ImmutablePair<Long, List<String>> longListImmutablePair = handle.searchMediaTotalHandle(boolQuery,
                sortBuilder,
                liveReqMediaSearch.getStart(),
                liveReqMediaSearch.getEnd() - liveReqMediaSearch.getStart(),
                LIVEESTYPE);
        if (longListImmutablePair == null){
            return null;
        }

        EsRspResult esRspResult = new EsRspResult();
        esRspResult.setTotalNum(longListImmutablePair.getLeft());
        esRspResult.setMediaList(longListImmutablePair.getRight());
        return esRspResult;
    }
}
