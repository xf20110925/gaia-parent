package com.ptb.gaia.index.bean;

import java.util.List;

/**
 * Created by watson zhang on 2017/3/28.
 */
public class WbReqMediaIndex extends ReqMediaBasicIndex {
    List<Integer> iIds;

    int fansNum;

    int avgSpread;       //平均转发数

    int avgComment;    //平均评论数

    int avgLike;       //平均点赞数

    boolean ifBrief;    //是否有简介

    public List<Integer> getiIds() {
        return iIds;
    }

    public void setiIds(List<Integer> iIds) {
        this.iIds = iIds;
    }

    public int getFansNum() {
        return fansNum;
    }

    public void setFansNum(int fansNum) {
        this.fansNum = fansNum;
    }

    public int getAvgSpread() {
        return avgSpread;
    }

    public void setAvgSpread(int avgSpread) {
        this.avgSpread = avgSpread;
    }

    public int getAvgComment() {
        return avgComment;
    }

    public void setAvgComment(int avgComment) {
        this.avgComment = avgComment;
    }

    public int getAvgLike() {
        return avgLike;
    }

    public void setAvgLike(int avgLike) {
        this.avgLike = avgLike;
    }

    public boolean isIfBrief() {
        return ifBrief;
    }

    public void setIfBrief(boolean ifBrief) {
        this.ifBrief = ifBrief;
    }
}
