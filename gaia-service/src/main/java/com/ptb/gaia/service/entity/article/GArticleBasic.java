package com.ptb.gaia.service.entity.article;

import java.util.*;

/**
 *
 */
public class GArticleBasic {

    /**
     * DefaultPeroidPoint constructor
     */
    public GArticleBasic() {
    }

    /**
     *
     */
    private String title;

    /**
     *
     */
    private String content;

    /**
     *
     */
    private Long postTime;

    /**
     *
     */
    private String pmid;

    /**
     *
     */
    private String coverPlan;

    /**
     *
     */
    private String articleUrl;

    /**
     *
     */
    private Long addTime;


    /**
     *
     */
    private Long updateTime = System.currentTimeMillis();



    /**
     *
     */
    private List<HotToken> hotToken;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Long getPostTime() {
        return postTime;
    }

    public void setPostTime(Long postTime) {
        this.postTime = postTime;
    }

    public String getPmid() {
        return pmid;
    }

    public void setPmid(String pmid) {
        this.pmid = pmid;
    }

    public String getCoverPlan() {
        return coverPlan;
    }

    public void setCoverPlan(String coverPlan) {
        this.coverPlan = coverPlan;
    }

    public String getArticleUrl() {
        return articleUrl;
    }

    public void setArticleUrl(String articleUrl) {
        this.articleUrl = articleUrl;
    }

    public Long getAddTime() {
        return addTime;
    }

    public void setAddTime(Long addTime) {
        this.addTime = addTime;
    }

    public List<HotToken> getHotToken() {
        return hotToken;
    }

    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }

    public void setHotToken(List<HotToken> hotToken) {
        this.hotToken = hotToken;
    }
}