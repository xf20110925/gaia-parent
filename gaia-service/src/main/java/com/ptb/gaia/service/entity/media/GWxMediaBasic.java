package com.ptb.gaia.service.entity.media;

/**
 *
 */
public class GWxMediaBasic extends GMediaBasic {

    /**
     * DefaultPeroidPoint constructor
     */
    public GWxMediaBasic() {
    }

    /**
     *
     */
    private Integer isAuth = 0;

    /**
     *
     */
    private String authInfo = "";

    /**
     *
     */
    private String qrcode = "";

    /**
     *
     */
    private String weixinId = "";
    /**
     * 微信媒体粉丝数
     */
    private Integer fansNum = -1;

    public Integer getIsAuth() {
        return isAuth;
    }

    public void setIsAuth(Integer isAuth) {
        this.isAuth = isAuth;
    }

    public String getAuthInfo() {
        return authInfo;
    }

    public void setAuthInfo(String authInfo) {
        this.authInfo = authInfo;
    }

    public String getQrcode() {
        return qrcode;
    }

    public void setQrcode(String qrcode) {
        this.qrcode = qrcode;
    }

    public String getWeixinId() {
        return weixinId;
    }

    public void setWeixinId(String weixinId) {
        this.weixinId = weixinId;
    }

    public Integer getFansNum() {
        return fansNum;
    }

    public void setFansNum(Integer fansNum) {
        this.fansNum = fansNum;
    }
}