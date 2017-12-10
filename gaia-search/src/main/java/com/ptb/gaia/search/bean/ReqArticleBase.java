package com.ptb.gaia.search.bean;

/**
 * Created by watson zhang on 2017/3/13.
 */
public class ReqArticleBase extends StartEnd{
    int mediaType;//微信 or 微博

    int searchType;//title or content

    String keyword;

    int sortType;

    public int getMediaType() {
        return mediaType;
    }

    public void setMediaType(int mediaType) {
        this.mediaType = mediaType;
    }

    public int getSearchType() {
        return searchType;
    }

    public void setSearchType(int searchType) {
        this.searchType = searchType;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public int getSortType() {
        return sortType;
    }

    public void setSortType(int sortType) {
        this.sortType = sortType;
    }
}
