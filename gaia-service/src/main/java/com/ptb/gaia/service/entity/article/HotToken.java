package com.ptb.gaia.service.entity.article;

import java.io.Serializable;

/**
 *
 */
public class HotToken implements Serializable{

    /**
     * DefaultPeroidPoint constructor
     */
    public HotToken() {
    }

    /**
     *
     */
    private String token;

    /**
     *
     */
//    private String freq;

    /**
     *
     */
    private String score;

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

   /* public String getFreq() {
        return freq;
    }

    public void setFreq(String freq) {
        this.freq = freq;
    }*/

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }
}