package com.ptb.gaia.service.entity.media;

/**
 *
 */
public class GWbMediaBasic extends GMediaBasic {

    public final static Integer Fmale = 0;
    public final static Integer Male = 1;

    /**
     * DefaultPeroidPoint constructor
     */
    public GWbMediaBasic() {
    }

    /**
     *
     */
    private Integer isAuth = 0;

    /**
     *
     */
    private String authInfo = "";

    private String location = "";
    private Long regiterTime = 0L;
    private Integer gender;
    private Integer fansNum = -1;
    private Integer postNum = -1;

    public Integer getFansNum() {
        return fansNum;
    }

    public Integer getPostNum() {
        return postNum;
    }

    public void setFansNum(Integer fansNum) {
        this.fansNum = fansNum;
    }

    public void setPostNum(Integer postNum) {
        this.postNum = postNum;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Long getRegiterTime() {
        return regiterTime;
    }

    public void setRegiterTime(Long regiterTime) {
        this.regiterTime = regiterTime;
    }

    public Integer getGender() {
        return gender;
    }

    public void setGender(Integer gender) {
        this.gender = gender;
    }

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
}