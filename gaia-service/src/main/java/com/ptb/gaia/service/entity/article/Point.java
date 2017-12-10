package com.ptb.gaia.service.entity.article;

import java.io.Serializable;

/**
 *
 */
public class Point implements Serializable{

    /**
     * DefaultPeroidPoint constructor
     */
    public Point() {
    }

    /**
     *
     */
    private Long time;

    /**
     *
     */
    private Long value;


    public Point(long timeInMillis, int i) {
        this.time = timeInMillis;
        this.value = Long.valueOf(i);
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }
}