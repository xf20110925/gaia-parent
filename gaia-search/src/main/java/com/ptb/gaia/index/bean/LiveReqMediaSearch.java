package com.ptb.gaia.index.bean;

/**
 * Created by watson zhang on 2017/4/1.
 */
public class LiveReqMediaSearch extends ReqMediaBasicSearch{

    int minFans;

    int maxFans;

    int minOnline;

    int maxOnline;

    String location;

    int gender;

    public int getMinFans() {
        return minFans;
    }

    public void setMinFans(int minFans) {
        this.minFans = minFans;
    }

    public int getMaxFans() {
        return maxFans;
    }

    public void setMaxFans(int maxFans) {
        this.maxFans = maxFans;
    }

    public int getMinOnline() {
        return minOnline;
    }

    public void setMinOnline(int minOnline) {
        this.minOnline = minOnline;
    }

    public int getMaxOnline() {
        return maxOnline;
    }

    public void setMaxOnline(int maxOnline) {
        this.maxOnline = maxOnline;
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
}
