package com.ptb.gaia.search.media;

import com.ptb.gaia.index.bean.WxReqMediaSearch;
import com.ptb.gaia.search.utils.EsRspResult;
import com.ptb.gaia.service.entity.media.GWxMedia;

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
 * Created by watson zhang on 16/7/19.
 */
public class WeixinMediaSearch {
    private static final Logger logger = LoggerFactory.getLogger(WeiboMediaSearch.class);
    String WXESTYPE = "weixinMedia";
    MediaSearchHandle handle;

    public WeixinMediaSearch(){
        this.handle = MediaSearchHandle.getInstance();
    }

    private SortBuilder getTagsSort(String mediaName){
        String scriptWeixin = String.format("if(doc['tags'].value != null) { scoreTmp = 1000.0} else { scoreTmp = 0.0};" +
                "return _score*(doc['mediaScore'].value - 0.5*abs(%d - doc['mediaNameLen'].value)) + scoreTmp;", mediaName.length());
                //"return _score*(doc['mediaScore'].value + 0.5*%d) + scoreTmp;", mediaName.length());
        Script script = new Script(scriptWeixin);
        return SortBuilders.scriptSort(script, "number").order(SortOrder.DESC).missing("_last");
    }

    public List<GWxMedia> searchWeixinMediaTags(String mediaName, int startPos, int endPos){
        if(mediaName == null || endPos <= startPos){
            return null;
        }
        try {
            QueryBuilder qb = QueryBuilders.boolQuery()
                    .should(matchQuery("mediaName", mediaName))
                    .should(QueryBuilders.termQuery("tags", mediaName)).boost(10f);
            SortBuilder sort = this.getTagsSort(mediaName);
            List<String> wbIdList = handle.searchMediaHandle(qb, sort, startPos, endPos-startPos, WXESTYPE);
            return handle.getWeixinMediaInfo(wbIdList);
        } catch (Exception e){
            logger.error("mediaName: {}, startPos: {}, endPos: {}", mediaName, startPos, endPos);
            return null;
        }
    }

    private SortBuilder getSort(String mediaName){
        //String scriptWeixin = String.format("return _score*(doc['mediaScore'].value + 0.5*%d);", mediaName.length());
        //String scriptWeixin = String.format("return _score*(doc['mediaScore'].value - 0.5*abs(%d - doc['mediaNameLen'].value));", mediaName.length());
        String scriptWeixin = String.format("return _score*(doc['mediaScore'].value);", mediaName.length());
        Script script = new Script(scriptWeixin);
        return SortBuilders.scriptSort(script, "number").order(SortOrder.DESC);
    }

    public List<GWxMedia> searchWeixinMediaInfo(String mediaName, int startPos, int endPos) {
        if(mediaName == null || endPos <= startPos){
            return null;
        }
        try {
            QueryBuilder qb = matchQuery("mediaName", mediaName);
            SortBuilder sort = this.getSort(mediaName);
            List<String> wbIdList = handle.searchMediaHandle(qb, sort, startPos, endPos-startPos, WXESTYPE);
            return handle.getWeixinMediaInfo(wbIdList);
        } catch (Exception e){
            logger.error("mediaName: {}, startPos: {}, endPos: {}", mediaName, startPos, endPos);
            return null;
        }
    }

    public GWxMedia searchWeixinMediaByExact(String mediaName){
        if(mediaName == null){
            return null;
        }
        try {
            QueryBuilder qb = QueryBuilders.termQuery("wxId", mediaName);
            List<String> wbIdList = handle.searchMediaHandle(qb, null, 0, 1, WXESTYPE);
            List<GWxMedia> wxMediaList = handle.getWeixinMediaInfo(wbIdList);
            if(wxMediaList.size() > 0){
                return wxMediaList.get(0);
            }else {
                return null;
            }
        } catch (Exception e){
            logger.error("mediaName: {}, startPos: {}, endPos: {}", mediaName, 0, 1);
            logger.error("{}", e);
            return null;
        }
    }

    public List<GWxMedia> searchWeixinMediaExactAndTags(String mediaName, int startPos, int endPos){
        if(mediaName == null || endPos <= startPos){
            return null;
        }
        try {
            List<GWxMedia> mediaList = this.searchWeixinMediaTags(mediaName, startPos, endPos);
            GWxMedia media = this.searchWeixinMediaByExact(mediaName);
            if(media == null){
                return mediaList;
            } else {
                for (GWxMedia m:mediaList){
                    if(m.getPmid().equals(media.getPmid())){
                        mediaList.remove(m);
                        break;
                    }
                }
                mediaList.add(media);
                return mediaList;
            }
        }catch (Exception e){
            logger.error("error info: mediaName: {}, startPos: {}, endPos: {}", mediaName, startPos, endPos);
            logger.error("{}", e);
            return null;
        }
    }

    @Deprecated /*去掉sType参数，tags搜索去掉了*/
    public EsRspResult<GWxMedia> searchWeixinMedia(int sType, String mediaName, int start, int end){
        if(start > end || mediaName == null){
            return null;
        }
        QueryBuilder qb;
        SortBuilder sort;
        if(sType == 1){
            //APP查询接口
            qb = matchQuery("mediaName", mediaName);
            sort = this.getSort(mediaName);

        }else if (sType == 2){
            //公众号查询接口
            qb = QueryBuilders.boolQuery()
                    .should(matchQuery("mediaName", mediaName))
                    .should(QueryBuilders.termQuery("tags", mediaName)).boost(10f);
            sort = this.getTagsSort(mediaName);
        }else {
            return null;
        }
        EsRspResult<GWxMedia> esRspResult = this.searchWeixinInfo(qb, sort, start, end);
        return esRspResult;
    }

    public EsRspResult<GWxMedia> searchWeixinMedia(String mediaName, int start, int end){
        if(start > end || mediaName == null){
            return null;
        }
        QueryBuilder qb;
        SortBuilder sort;
        //APP查询接口
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        qb = QueryBuilders.termQuery("wxId", mediaName);
        boolQueryBuilder.should(qb);
        qb = matchQuery("mediaName", mediaName);
        boolQueryBuilder.should(qb);
        sort = this.getSort(mediaName);
        EsRspResult<GWxMedia> esRspResult = this.searchWeixinInfo(boolQueryBuilder, sort, start, end);
        return esRspResult;
    }

    public EsRspResult<GWxMedia> searchWeixinInfo(QueryBuilder qb, SortBuilder sort, int start, int end){
        if(qb == null || sort == null){
            return null;
        }
        EsRspResult<GWxMedia> esRspResult = new EsRspResult<>();
        ImmutablePair<Long, List<String>> wxIdList = handle.searchMediaTotalHandle(qb, sort, start, end-start, WXESTYPE);
        if (wxIdList == null){
            return null;
        }
        esRspResult.setMediaList(handle.getWeixinMediaInfo(wxIdList.getRight()));
        esRspResult.setTotalNum(wxIdList.getLeft());

        return esRspResult;
    }


    public EsRspResult<String> searchWxInfoForWeb(WxReqMediaSearch wxReqMediaSearch){
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        if (wxReqMediaSearch.getKey() != null && wxReqMediaSearch.getKey().length() > 0){
            MatchQueryBuilder query = QueryBuilders.matchQuery("mediaName", wxReqMediaSearch.getKey());
            boolQuery.must(query);
        }
        boolean isHave = false;

        RangeQueryBuilder priceQuery = QueryBuilders.rangeQuery("price");
        if (wxReqMediaSearch.getMinPrice() > 0){
            priceQuery.gte(wxReqMediaSearch.getMinPrice()*100);
            isHave = true;
        }
        if (wxReqMediaSearch.getMaxPrice() > 0){
            priceQuery.lte(wxReqMediaSearch.getMaxPrice()*100);
            isHave = true;
        }
        if (isHave == true){
            boolQuery.filter(priceQuery);
            isHave = false;
        }

        if (wxReqMediaSearch.getClassify() != 0){
            TermQueryBuilder classifyQuery = QueryBuilders.termQuery("iId", wxReqMediaSearch.getClassify());
            boolQuery.filter(classifyQuery);
        }

        RangeQueryBuilder readQuery = QueryBuilders.rangeQuery("hlavgRead");
        if (wxReqMediaSearch.getMinRead() > 0){
            readQuery.gte(wxReqMediaSearch.getMinRead());
            isHave = true;
        }
        if (wxReqMediaSearch.getMaxRead() > 0){
            readQuery.lte(wxReqMediaSearch.getMaxRead());
            isHave = true;
        }
        if (isHave == true){
            boolQuery.filter(readQuery);
            isHave = false;
        }

        RangeQueryBuilder likeQuery = QueryBuilders.rangeQuery("hlavgZan");
        if (wxReqMediaSearch.getMinLike() > 0){
            likeQuery.gte(wxReqMediaSearch.getMinLike());
            isHave = true;
        }
        if (wxReqMediaSearch.getMaxLike() > 0){
            likeQuery.lte(wxReqMediaSearch.getMaxLike());
            isHave = true;
        }
        if (isHave == true){
            boolQuery.filter(likeQuery);
            isHave = false;
        }

        if (wxReqMediaSearch.getIsAuth() == 1){
            boolQuery.filter(QueryBuilders.termQuery("isAuth", 1));
        }else if (wxReqMediaSearch.getIsAuth() == 2)
            boolQuery.filter(QueryBuilders.termQuery("isAuth", 0));

        if (wxReqMediaSearch.getOriginal() == 1){
            boolQuery.filter(QueryBuilders.termQuery("isOriginal", 1));
        }else if (wxReqMediaSearch.getOriginal() == 2)
            boolQuery.filter(QueryBuilders.termQuery("isOriginal", 0));

        if (wxReqMediaSearch.getSeller() == 1){
            boolQuery.filter(QueryBuilders.rangeQuery("agentNum").gt(0));
        }else if (wxReqMediaSearch.getSeller() == 2){
            boolQuery.filter(QueryBuilders.rangeQuery("agentNum").lte(0));
        }

        FieldSortBuilder sortBuilder = null;
        if (wxReqMediaSearch.getSort() == 1){
            sortBuilder = null;
        } else if (wxReqMediaSearch.getSort() == 2){
            sortBuilder = SortBuilders.fieldSort("hlavgRead").order(SortOrder.DESC);
        } else if (wxReqMediaSearch.getSort() == 3){
            sortBuilder = SortBuilders.fieldSort("agentNum").order(SortOrder.DESC);
        }else if (wxReqMediaSearch.getSort() == 4){
            sortBuilder = SortBuilders.fieldSort("price").order(SortOrder.DESC);
        }
        ImmutablePair<Long, List<String>> longListImmutablePair = handle.searchMediaTotalHandle(boolQuery,
                sortBuilder,
                wxReqMediaSearch.getStart(),
                wxReqMediaSearch.getEnd() - wxReqMediaSearch.getStart(),
                WXESTYPE);
        if (longListImmutablePair == null){
            return null;
        }

        EsRspResult esRspResult = new EsRspResult();
        esRspResult.setTotalNum(longListImmutablePair.getLeft());
        esRspResult.setMediaList(longListImmutablePair.getRight());
        return esRspResult;

    }

    //for test
    public EsRspResult<GWxMedia> searchWeixinMediaTest(String mediaName, int start, int end){
        if(start > end || mediaName == null){
            return null;
        }
        QueryBuilder qb;
        SortBuilder sort = null;
        //APP查询接口
        //BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        qb = QueryBuilders.termQuery("iId", mediaName);
        //boolQueryBuilder.should(qb);
        sort = this.getSort(mediaName);
        EsRspResult<GWxMedia> esRspResult = this.searchWeixinInfo(qb, sort, start, end);
        return esRspResult;
    }
}
