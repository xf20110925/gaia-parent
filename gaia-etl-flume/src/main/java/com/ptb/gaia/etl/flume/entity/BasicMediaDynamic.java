package com.ptb.gaia.etl.flume.entity;

/**
 * Created by watson zhang on 16/4/29.
 */
public class BasicMediaDynamic {
    long time;
    int plat;
    int mediaName;
    int fans;
    int postArticles;
    int concerns;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public int getPlat() {
        return plat;
    }

    public void setPlat(int plat) {
        this.plat = plat;
    }

    public int getFans() {
        return fans;
    }

    public void setFans(int fans) {
        this.fans = fans;
    }

    public int getPostArticles() {
        return postArticles;
    }

    public void setPostArticles(int postArticles) {
        this.postArticles = postArticles;
    }

    public int getConcerns() {
        return concerns;
    }

    public void setConcerns(int concerns) {
        this.concerns = concerns;
    }
}
