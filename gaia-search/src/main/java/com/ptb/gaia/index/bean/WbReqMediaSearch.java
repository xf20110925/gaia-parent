package com.ptb.gaia.index.bean;

/**
 * Created by watson zhang on 2017/4/1.
 */
public class WbReqMediaSearch extends ReqMediaBasicSearch{
    int minFans;

    int maxFans;

    int minForward;

    int maxForward;

    int minComment;

    int maxComment;

    int minLike;

    int maxLike;

    public int getMinFans() {
        return minFans;
    }

    public void setMinFans(int minFans) {
        this.minFans = minFans;
    }

    public int getMaxFans() {
        return maxFans;
    }

    public void setMaxFans(int maxFans) {
        this.maxFans = maxFans;
    }

    public int getMinForward() {
        return minForward;
    }

    public void setMinForward(int minForward) {
        this.minForward = minForward;
    }

    public int getMaxForward() {
        return maxForward;
    }

    public void setMaxForward(int maxForward) {
        this.maxForward = maxForward;
    }

    public int getMinComment() {
        return minComment;
    }

    public void setMinComment(int minComment) {
        this.minComment = minComment;
    }

    public int getMaxComment() {
        return maxComment;
    }

    public void setMaxComment(int maxComment) {
        this.maxComment = maxComment;
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
