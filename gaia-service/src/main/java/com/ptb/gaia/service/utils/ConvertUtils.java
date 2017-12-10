package com.ptb.gaia.service.utils;

import com.ptb.gaia.service.entity.media.GWbMedia;
import com.ptb.gaia.service.entity.media.GWxMedia;
import com.ptb.uranus.spider.weibo.bean.WeiboAccount;
import com.ptb.uranus.spider.weixin.bean.WxAccount;

/**
 * @DESC:
 * @VERSION:
 * @author: xuefeng
 * @Date: 2016/7/25
 * @Time: 18:09
 */
public class ConvertUtils {
    static class ConvertException extends RuntimeException {
        public ConvertException(String message) {
            super(message);
        }
    }

    public static GWxMedia convert2GWxMedia(WxAccount wxAccount) {
        try {
            GWxMedia gWxMedia = new GWxMedia();
            gWxMedia.setWeixinId(wxAccount.getId());
            gWxMedia.setBrief(wxAccount.getBrief());
            gWxMedia.setQrcode(wxAccount.getQcCode());
            gWxMedia.setMediaName(wxAccount.getNickName());
            gWxMedia.setPmid(wxAccount.getBiz());
            gWxMedia.setHeadImage(wxAccount.getHeadImg());
            return gWxMedia;
        } catch (Exception e) {
            throw new ConvertException("convert wxAccount fail");
        }
    }

    public static GWbMedia convert2GWbMedia(WeiboAccount wbAccount) {
        try {
            GWbMedia gWbMedia = new GWbMedia();
            gWbMedia.setPmid(wbAccount.getWeiboID());
            gWbMedia.setBrief(wbAccount.getDesc());
            gWbMedia.setMediaName(wbAccount.getNickName());
//            gWbMedia.setFansNum(wbAccount.getFansNum());
//            gWbMedia.setPostNum(wbAccount.getAttNum());
            gWbMedia.setHeadImage(wbAccount.getHeadImg());
            gWbMedia.setAuthInfo(wbAccount.getVerifiedReason());
            gWbMedia.setLocation(wbAccount.getActivePlace());
            gWbMedia.setGender(wbAccount.getGender().equals("male") ? 0 : 1);
            gWbMedia.setIsAuth(Integer.parseInt(wbAccount.getVerifiedType()));
            gWbMedia.setRegiterTime(wbAccount.getCreate_time());
            return gWbMedia;
        } catch (Exception e) {
            throw new ConvertException("convert wbAccount fail");
        }
    }


    public static GWbMedia ConverDynamic(WeiboAccount weiboAccount){
        try {
            GWbMedia gWbMedia = new GWbMedia();
            gWbMedia.setFansNum(weiboAccount.getFansNum());
            gWbMedia.setPostNum(weiboAccount.getAttNum());
            return gWbMedia;

        }catch (Exception e){
            throw new ConvertException("conver WBDynamic fail ");
        }

    }
}
