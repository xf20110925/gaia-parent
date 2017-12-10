package com.ptb.gaia.index.bean;

/**
 * Created by watson zhang on 2017/3/28.
 */
public class ReqMediaBasicIndex {
    String mediaName;

    int platType;

    int mediaType;

    String pmid;

    long price;

    int agentNum;   //代理数

    int isAuth;     //是否认证

    public String getMediaName() {
        return mediaName;
    }

    public void setMediaName(String mediaName) {
        this.mediaName = mediaName;
    }

    public int getPlatType() {
        return platType;
    }

    public void setPlatType(int platType) {
        this.platType = platType;
    }

    public int getMediaType() {
        return mediaType;
    }

    public void setMediaType(int mediaType) {
        this.mediaType = mediaType;
    }

    public String getPmid() {
        return pmid;
    }

    public void setPmid(String pmid) {
        this.pmid = pmid;
    }

    public long getPrice() {
        return price;
    }

    public void setPrice(long price) {
        this.price = price;
    }

    public int getAgentNum() {
        return agentNum;
    }

    public void setAgentNum(int agentNum) {
        this.agentNum = agentNum;
    }

    public int getIsAuth() {
        return isAuth;
    }

    public void setIsAuth(int isAuth) {
        this.isAuth = isAuth;
    }
}
