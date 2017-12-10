package com.ptb.gaia.service.entity.article;



/**
 *
 */
public class GWxArticleDynamic {

    /**
     * DefaultPeroidPoint constructor
     */
    public GWxArticleDynamic() {
    }

    /**
     *
     */
    private Integer readNum;

    /**
     *
     */
    private Integer likeNum;

    /**
     *
     */
    private long addTime;

    /**
     *
     */
    private String articleUrl;

    public GWxArticleDynamic(String articleUrl,Integer readNum, Integer likeNum) {
        this.articleUrl = articleUrl;
        this.readNum = readNum;
        this.likeNum = likeNum;
        this.addTime = System.currentTimeMillis();
    }




    public Integer getReadNum() {
        return readNum;
    }

    public void setReadNum(Integer readNum) {
        this.readNum = readNum;
    }

    public Integer getLikeNum() {
        return likeNum;
    }

    public void setLikeNum(Integer likeNum) {
        this.likeNum = likeNum;
    }

    public long getAddTime() {
        return addTime;
    }

    public void setAddTime(long addTime) {
        this.addTime = addTime;
    }

    public String getArticleUrl() {
        return articleUrl;
    }

    public void setArticleUrl(String articleUrl) {
        this.articleUrl = articleUrl;
    }
}