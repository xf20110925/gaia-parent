package com.ptb.gaia.service.entity.article;


/**
 *
 */
public class GWbArticleDynamic {

    /**
     * DefaultPeroidPoint constructor
     */
    public GWbArticleDynamic() {
    }

    /**
     *
     */
    private Integer likeNum;

    /**
     *
     */
    private Integer forwardNum;

    /**
     *
     */
    private Integer commentNum;

    private Integer readNum;

    /**
     *
     */
    private long addTime;

    /**
     *
     */
    private String articleUrl;


    public GWbArticleDynamic(String articleUrl,Integer likeNum, Integer forwardNum, Integer commentNum) {
        this.likeNum = likeNum;
        this.forwardNum = forwardNum;
        this.commentNum = commentNum;
        this.articleUrl = articleUrl;
        this.addTime = System.currentTimeMillis();
    }

    public Integer getLikeNum() {
        return likeNum;
    }

    public void setLikeNum(Integer likeNum) {
        this.likeNum = likeNum;
    }

    public Integer getForwardNum() {
        return forwardNum;
    }

    public void setForwardNum(Integer forwardNum) {
        this.forwardNum = forwardNum;
    }

    public Integer getCommentNum() {
        return commentNum;
    }

    public void setCommentNum(Integer commentNum) {
        this.commentNum = commentNum;
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

    public Integer getReadNum() {
        return readNum;
    }

    public void setReadNum(Integer readNum) {
        this.readNum = readNum;
    }
}