package com.ptb.gaia.tool.entity;


import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;


/**
 * Created by xuefeng on 2016/5/17.
 */
public class BayouWXMedia {
    private String code;
    private String qrcode;
    private String name;
    //    private String id;
    private String bid;
    private String info;
    private String headimage = "";
    private String authentication;
    private boolean original = false;

    public BayouWXMedia() {
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
        if (StringUtils.isNotBlank(code)) {
            headimage = "http://open.weixin.qq.com/qr/code/?username=" + code;
        }
    }

    public String getQrcode() {
        return qrcode;
    }

    public void setQrcode(String qrcode) {
        this.qrcode = qrcode;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBid() {
        return bid;
    }

    public void setBid(String bid) {
        this.bid = bid;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public String getHeadimage() {
        return headimage;
    }

    public void setHeadimage(String headimage) {
        this.headimage = headimage;
    }

    public String getAuthentication() {
        return authentication;
    }

    public void setAuthentication(String authentication) {
        this.authentication = authentication;
    }

    public boolean isOriginal() {
        return original;
    }

    public void setOriginal(boolean original) {
        this.original = original;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
