package com.ptb.gaia.index.bean;

import java.util.List;

/**
 * Created by watson zhang on 2017/3/24.
 */
public class WxReqMediaIndex extends ReqMediaBasicIndex {
    List<Integer> iIds;

    int hlavgRead;  //头条平均阅读数

    int hlavgZan;   //头条平均点赞数

    int isOriginal;

    int pi;

    String wxId;

    boolean ifBrief;    //是否有简介

    public List<Integer> getiIds() {
        return iIds;
    }

    public void setiIds(List<Integer> iIds) {
        this.iIds = iIds;
    }

    public int getHlavgRead() {
        return hlavgRead;
    }

    public void setHlavgRead(int hlavgRead) {
        this.hlavgRead = hlavgRead;
    }

    public int getHlavgZan() {
        return hlavgZan;
    }

    public void setHlavgZan(int hlavgZan) {
        this.hlavgZan = hlavgZan;
    }

    public int getIsOriginal() {
        return isOriginal;
    }

    public void setIsOriginal(int isOriginal) {
        this.isOriginal = isOriginal;
    }

    public int getPi() {
        return pi;
    }

    public void setPi(int pi) {
        this.pi = pi;
    }

    public String getWxId() {
        return wxId;
    }

    public void setWxId(String wxId) {
        this.wxId = wxId;
    }

    public boolean isIfBrief() {
        return ifBrief;
    }

    public void setIfBrief(boolean ifBrief) {
        this.ifBrief = ifBrief;
    }
}
