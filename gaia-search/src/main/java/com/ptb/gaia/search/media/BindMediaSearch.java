package com.ptb.gaia.search.media;

import com.ptb.gaia.search.utils.EsRspResult;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

/**
 * 提供以后搜索绑定媒体功能，需要将绑定媒体信息放入ES中。
 * Created by watson zhang on 2017/3/24.
 */
public class BindMediaSearch {
    private static final Logger logger = LoggerFactory.getLogger(BindMediaSearch.class);
    String BINDESTYPE = "bindMedia";
    MediaSearchHandle handle;

    public BindMediaSearch(){
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
        QueryBuilder qb = matchQuery("productName", mediaName);
        SortBuilder sort = this.getSort(mediaName);
        EsRspResult<String> esRspResult = new EsRspResult<>();
        ImmutablePair<Long, List<String>> wxIdList = handle.searchMediaTotalHandle(qb, sort, start, end-start, BINDESTYPE);
        if (wxIdList == null){
            return null;
        }
        esRspResult.setTotalNum(wxIdList.getLeft());
        esRspResult.setMediaList(wxIdList.getRight());

        return esRspResult;
    }
}
