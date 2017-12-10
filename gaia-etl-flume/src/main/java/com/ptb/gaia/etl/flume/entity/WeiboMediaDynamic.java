package com.ptb.gaia.etl.flume.entity;

/**
 * Created by watson zhang on 16/4/29.
 */
public class WeiboMediaDynamic extends BasicMediaDynamic{
    String weiboId;

    public String getWeiboId() {
        return weiboId;
    }

    public void setWeiboId(String weiboId) {
        this.weiboId = weiboId;
    }
}
