package com.ptb.gaia.search.utils;

import java.util.List;

/**
 * Created by watson zhang on 16/9/8.
 */
public class RspBasic<T> {
    List<T> mediaList;

    long totalNum;

    public List<T> getMediaList() {
        return mediaList;
    }

    public void setMediaList(List<T> mediaList) {
        this.mediaList = mediaList;
    }

    public long getTotalNum() {
        return totalNum;
    }

    public void setTotalNum(long totalNum) {
        this.totalNum = totalNum;
    }
}
