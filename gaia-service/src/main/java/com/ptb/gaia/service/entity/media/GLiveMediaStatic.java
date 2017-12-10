package com.ptb.gaia.service.entity.media;

import com.ptb.gaia.service.entity.article.Point;

import java.io.Serializable;
import java.util.List;

/**
 * Created by liujianpeng on 2017/2/27 0027.
 */
public class GLiveMediaStatic extends GMediaBasic implements Serializable{

    private int plat;//平台 3 直播
    private String mediaName;//媒体名称
    private String headImage;//头像
    private int platIcon;//平台图标 3 映客，4 一直播 ，5 斗鱼， 6花椒
    private int isVip;//是否认证
    private int fansNum;//粉丝数
    private String introduction;//简介
    private String location;//地域
    private String age;//年龄
    private String accountLevel;//平台等级
    private int gender;//性别
    private String accountId;//房间号或者账号ID
    private String homePage;//视频历史主页地址
    private String detailUrl;//个人主页
    private String liveMedia;//平台 --斗鱼、映客、一直播...
    private String verificationInfo;//验证信息
    private Integer maxLiveUserNum;    //最高直播观众数
    private Integer avgLiveUserNum;    //平均直播观众数
    private Double totalReward;        //累计收到的打赏
    private Double totalAmount;        //累计收入
    private Integer totalLikeNum;      //累计点赞
    private List<Point> fansData;      //粉丝变化趋势 time时间 value 粉丝数
    private String pmid;

    private List<String> tags;
    private Long addTime;

    public int getPlat() {
        return plat;
    }

    public void setPlat(int plat) {
        this.plat = plat;
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
        this.headImage = headImage;
    }

    public int getPlatIcon() {
        return platIcon;
    }

    public void setPlatIcon(int platIcon) {
        this.platIcon = platIcon;
    }

    public int getIsVip() {
        return isVip;
    }

    public void setIsVip(int isVip) {
        this.isVip = isVip;
    }

    public int getFansNum() {
        return fansNum;
    }

    public void setFansNum(int fansNum) {
        this.fansNum = fansNum;
    }

    public String getIntroduction() {
        return introduction;
    }

    public void setIntroduction(String introduction) {
        this.introduction = introduction;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getAccountLevel() {
        return accountLevel;
    }

    public void setAccountLevel(String accountLevel) {
        this.accountLevel = accountLevel;
    }

    public int getGender() {
        return gender;
    }

    public void setGender(int gender) {
        this.gender = gender;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getHomePage() {
        return homePage;
    }

    public void setHomePage(String homePage) {
        this.homePage = homePage;
    }

    public String getDetailUrl() {
        return detailUrl;
    }

    public void setDetailUrl(String detailUrl) {
        this.detailUrl = detailUrl;
    }

    public String getLiveMedia() {
        return liveMedia;
    }

    public void setLiveMedia(String liveMedia) {
        this.liveMedia = liveMedia;
    }

    public String getVerificationInfo() {
        return verificationInfo;
    }

    public void setVerificationInfo(String verificationInfo) {
        this.verificationInfo = verificationInfo;
    }

    public Integer getMaxLiveUserNum() {
        return maxLiveUserNum;
    }

    public void setMaxLiveUserNum(Integer maxLiveUserNum) {
        this.maxLiveUserNum = maxLiveUserNum;
    }

    public Integer getAvgLiveUserNum() {
        return avgLiveUserNum;
    }

    public void setAvgLiveUserNum(Integer avgLiveUserNum) {
        this.avgLiveUserNum = avgLiveUserNum;
    }

    public Double getTotalReward() {
        return totalReward;
    }

    public void setTotalReward(Double totalReward) {
        this.totalReward = totalReward;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public Integer getTotalLikeNum() {
        return totalLikeNum;
    }

    public void setTotalLikeNum(Integer totalLikeNum) {
        this.totalLikeNum = totalLikeNum;
    }

    public List<Point> getFansData() {
        return fansData;
    }

    public void setFansData(List<Point> fansData) {
        this.fansData = fansData;
    }

    public String getPmid() {
        return pmid;
    }

    public void setPmid(String pmid) {
        this.pmid = pmid;
    }


    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Long getAddTime() {
        return addTime;
    }

    public void setAddTime(Long addTime) {
        this.addTime = addTime;
    }
}
