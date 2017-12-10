package com.ptb.gaia.service.entity.media;

import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class GMediaBasic {

    /**
     * DefaultPeroidPoint constructor
     */
    public GMediaBasic() {
    }

    /**
     *
     */
    private String pmid = "";

    /**
     *
     */
    private String mediaName = "";

    /**
     *
     */
    private String headImage = "";

    /**
     *
     */
    private String brief = "";

    /**
     *
     */
    private Integer category = -1;

    /**
     *
     */
    private Long addTime = System.currentTimeMillis();

    private Long updateTime = System.currentTimeMillis();

    private List<String> tags;
    private Long mediaId;//非mongo字段。mysql中媒体的唯一标识

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public String getPmid() {
        return pmid;
    }

    public void setPmid(String pmid) {
        this.pmid = pmid;
    }

    public String getMediaName() {
        return mediaName;
    }

    public void setMediaName(String mediaName) {
        this.mediaName = mediaName;
    }

    public String getHeadImage() {
        return headImage;
    }

    public void setHeadImage(String headImage) {
        try {
            if(headImage != null && headImage.contains("sinaimg")) {
                headImage = headImage.replaceFirst("\\d{1,3}/","100/");
            }
        }catch (Exception e) {

        }
        this.headImage = headImage;
    }

    public Integer getCategory() {
        return category;
    }

    public void setCategory(Integer category) {
        this.category = category;
    }

    public String getBrief() {
        return brief;
    }

    public void setBrief(String brief) {
        this.brief = brief;
    }

    public Long getAddTime() {
        return addTime;
    }

    public void setAddTime(Long addTime) {
        this.addTime = addTime;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Long getMediaId() {
        return mediaId;
    }

    public void setMediaId(Long mediaId) {
        this.mediaId = mediaId;
    }

    public static class RecentArticle implements Serializable {
        String value;
        Long time;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public Long getTime() {
            return time;
        }

        public void setTime(Long time) {
            this.time = time;
        }
    }

    RecentArticle latestArticle;


    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }

    public RecentArticle getLatestArticle() {
        return latestArticle;
    }

    public void setLatestArticle(RecentArticle latestArticle) {
        this.latestArticle = latestArticle;
    }

    public void reset() {

    }
}