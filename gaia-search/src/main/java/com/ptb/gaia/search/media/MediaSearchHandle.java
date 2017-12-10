package com.ptb.gaia.search.media;

import com.ptb.gaia.search.utils.MediaSearchUtil;
import com.ptb.gaia.service.IGaia;
import com.ptb.gaia.service.IGaiaImpl;
import com.ptb.gaia.service.entity.media.GWbMedia;
import com.ptb.gaia.service.entity.media.GWxMedia;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by watson zhang on 16/7/19.
 */
public class MediaSearchHandle {

    String USERIDNAME = "pmid";
    static IGaia iGaia;

    private static final MediaSearchHandle handle = new MediaSearchHandle();
    private MediaSearchHandle(){
        if(iGaia == null){
            try {
                iGaia = new IGaiaImpl();
            } catch (ConfigurationException e) {
                e.printStackTrace();
            }
        }
    }

    public static MediaSearchHandle getInstance(){
        return handle;
    }

    public List<GWbMedia> getWeiboMediaInfo(List<String> wbIdList){
        List<GWbMedia> wbMediaList;
        wbMediaList = iGaia.getWbMediaBatch(wbIdList);
        return wbMediaList;
    }

    public List<GWxMedia> getWeixinMediaInfo(List<String> wxBizList){
        List<GWxMedia> wbMediaList;
        wbMediaList = iGaia.getWxMediaBatch(wxBizList);
        return wbMediaList;
    }

    @Deprecated
    public List<String> searchMediaHandle(QueryBuilder qb, @Nullable SortBuilder sort, int startNum, int offset, String indexType){
        SearchHits searchHits = MediaSearchUtil.getInstanse().matchQueryMedia(qb, sort, startNum, offset, indexType);

        if (searchHits == null){
            return null;
        }
        List<String> wbIdList = new ArrayList<>();
        Iterator<SearchHit> iterator = searchHits.iterator();
        while (iterator.hasNext()){
            SearchHit searchHit = iterator.next();
            wbIdList.add((String)searchHit.getSource().get(USERIDNAME));
        }
        return wbIdList;
    }

    public ImmutablePair<Long, List<String>> searchMediaTotalHandle(QueryBuilder qb, @Nullable SortBuilder sort, int startNum, int offset, String indexType){
         SearchHits searchHits = MediaSearchUtil.getInstanse().matchQueryMedia(qb, sort, startNum, offset, indexType);

        if (searchHits == null){
            return null;
        }
        List<String> wbIdList = new ArrayList<>();
        Iterator<SearchHit> iterator = searchHits.iterator();
        while (iterator.hasNext()){
            SearchHit searchHit = iterator.next();
            wbIdList.add((String)searchHit.getSource().get(USERIDNAME));
        }

        ImmutablePair<Long, List<String>> immutablePair = new ImmutablePair<>(searchHits.getTotalHits(), wbIdList);
        return immutablePair;

    }

}
