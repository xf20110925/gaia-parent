package com.ptb.gaia.index.bean;

/**
 * Created by watson zhang on 2017/3/28.
 */
public class LiveReqMediaIndex extends ReqMediaBasicIndex {
    int avgOnlineNum;   //平均在线人数

    String location;    //位置

    int gender;

    int fansNum;

    public int getAvgOnlineNum() {
        return avgOnlineNum;
    }

    public void setAvgOnlineNum(int avgOnlineNum) {
        this.avgOnlineNum = avgOnlineNum;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public int getGender() {
        return gender;
    }

    public void setGender(int gender) {
        this.gender = gender;
    }

    public int getFansNum() {
        return fansNum;
    }

    public void setFansNum(int fansNum) {
        this.fansNum = fansNum;
    }
}
