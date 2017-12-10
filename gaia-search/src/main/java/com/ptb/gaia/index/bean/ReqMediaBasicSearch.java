package com.ptb.gaia.index.bean;

import java.io.Serializable;

/**
 * Created by watson zhang on 2017/4/1.
 */
public class ReqMediaBasicSearch implements Serializable{
    String key;

    int mediaType;

    int classify;

    long minPrice;

    long maxPrice;

    int seller;     //是否有代理

    int original;

    int isAuth;

    int sort;

    int start;

    int end;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getMediaType() {
        return mediaType;
    }

    public void setMediaType(int mediaType) {
        this.mediaType = mediaType;
    }

    public int getClassify() {
        return classify;
    }

    public void setClassify(int classify) {
        this.classify = classify;
    }

    public long getMinPrice() {
        return minPrice;
    }

    public void setMinPrice(long minPrice) {
        this.minPrice = minPrice;
    }

    public long getMaxPrice() {
        return maxPrice;
    }

    public void setMaxPrice(long maxPrice) {
        this.maxPrice = maxPrice;
    }

    public int getSeller() {
        return seller;
    }

    public void setSeller(int seller) {
        this.seller = seller;
    }

    public int getOriginal() {
        return original;
    }

    public void setOriginal(int original) {
        this.original = original;
    }

    public int getIsAuth() {
        return isAuth;
    }

    public void setIsAuth(int isAuth) {
        this.isAuth = isAuth;
    }

    public int getSort() {
        return sort;
    }

    public void setSort(int sort) {
        this.sort = sort;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }
}
