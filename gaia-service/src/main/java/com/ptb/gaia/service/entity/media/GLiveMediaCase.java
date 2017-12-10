package com.ptb.gaia.service.entity.media;

import java.io.Serializable;

/**
 * Created by zuokui.fu on 2017/3/2.
 * description: 媒体案例实体类
 */
public class GLiveMediaCase extends GMediaBasic implements Serializable {

    private String accountId;
    private int plat;
    private int platIcon;
    private String videoTitle;//视频标题
    private Long postTime;    //发布时间
    private String videoUrl;  //视频地址
    private Integer viewNum;  //观看数
    private Integer likeNum;  //点赞数
    private Integer commentNum; //评论数
    private String coverPlan;//封面图
    private String scid;
    private Long addTime;
    private String pmid;

    public String getScid() {
        return scid;
    }

    public void setScid(String scid) {
        this.scid = scid;
    }

    public String getCoverPlan() {
        return coverPlan;
    }

    public void setCoverPlan(String coverPlan) {
        this.coverPlan = coverPlan;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public int getPlat() {
        return plat;
    }

    public void setPlat(int plat) {
        this.plat = plat;
    }

    public int getPlatIcon() {
        return platIcon;
    }

    public void setPlatIcon(int platIcon) {
        this.platIcon = platIcon;
    }

    public String getVideoTitle() {
        return videoTitle;
    }

    public void setVideoTitle(String videoTitle) {
        this.videoTitle = videoTitle;
    }

    public Long getPostTime() {
        return postTime;
    }

    public void setPostTime(Long postTime) {
        this.postTime = postTime;
    }

    public String getVideoUrl() {
        return videoUrl;
    }

    public void setVideoUrl(String videoUrl) {
        this.videoUrl = videoUrl;
    }

    public Integer getViewNum() {
        return viewNum;
    }

    public void setViewNum(Integer viewNum) {
        this.viewNum = viewNum;
    }

    public Integer getLikeNum() {
        return likeNum;
    }

    public void setLikeNum(Integer likeNum) {
        this.likeNum = likeNum;
    }

    public Integer getCommentNum() {
        return commentNum;
    }

    public void setCommentNum(Integer commentNum) {
        this.commentNum = commentNum;
    }

    public Long getAddTime() {
        return addTime;
    }
    public void setAddTime(Long addTime) {
        this.addTime = addTime;
    }
}
