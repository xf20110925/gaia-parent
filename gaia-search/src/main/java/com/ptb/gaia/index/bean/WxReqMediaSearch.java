package com.ptb.gaia.index.bean;

/**
 * Created by watson zhang on 2017/4/1.
 */
public class WxReqMediaSearch extends ReqMediaBasicSearch{

    int minRead;

    int maxRead;

    int minLike;

    int maxLike;

    public int getMinRead() {
        return minRead;
    }

    public void setMinRead(int minRead) {
        this.minRead = minRead;
    }

    public int getMaxRead() {
        return maxRead;
    }

    public void setMaxRead(int maxRead) {
        this.maxRead = maxRead;
    }

    public int getMinLike() {
        return minLike;
    }

    public void setMinLike(int minLike) {
        this.minLike = minLike;
    }

    public int getMaxLike() {
        return maxLike;
    }

    public void setMaxLike(int maxLike) {
        this.maxLike = maxLike;
    }
}
