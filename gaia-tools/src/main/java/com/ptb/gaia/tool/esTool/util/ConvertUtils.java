package com.ptb.gaia.tool.esTool.util;

import com.ptb.uranus.spider.weibo.bean.WeiboAccount;
import com.ptb.uranus.spider.weixin.bean.GsData;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.bson.Document;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @DESC:
 * @VERSION:
 * @author: xuefeng
 * @Date: 2016/7/19
 * @Time: 10:39
 */
public class ConvertUtils {
    static int faildNum;
    static int BIGFANS = 5000000;
    static Pattern pattern = Pattern.compile(".*__biz=([^&]*).*");

    public static String[] convertTag2Array(String tags) {
        if (tags.indexOf("/") != -1) {
            return tags.split("/");
        } else {
            return new String[]{tags};
        }
    }

    public static ImmutablePair<String, XContentBuilder> convert2WxMedia(
            GsData gsWxMedia, String tag) {
        try {
            XContentBuilder contentMap = null;
            double srcScore = 1;
            double mediaNameCent = 0.5;
            Matcher matcher = pattern.matcher(gsWxMedia.getIncluded());
            String pmid = "";
            if (matcher.find()) {
                pmid = matcher.group(1);
            }
            if (StringUtils.isBlank(pmid)) return null;
            contentMap = JsonXContent.contentBuilder().startObject();
            contentMap = contentMap.field("pmid", pmid);

            String mediaName = gsWxMedia.getWechatname();
            int mediaNameLen = 0;
            if (mediaName != null) {
                contentMap = contentMap.field("mediaName", mediaName);
                mediaNameLen = mediaName.length();
            }

            contentMap = contentMap.field("timeStamp", new Date().getTime());

            Object isAuthString = gsWxMedia.getAuthenticationInfo();
            double isAuthCent = 0;
            if (isAuthString != null) {
                contentMap = contentMap.field("isAuth", 1);
                isAuthCent = 0.5;
            } else {
                contentMap = contentMap.field("isAuth", 0);
            }

            String weixinID = gsWxMedia.getWechatid();
            double weixinIDCent = 0;
            if (weixinID != null) {
                contentMap = contentMap.field("weixinID", weixinID);
            } else {
                weixinIDCent = 1;
            }

            String brief = gsWxMedia.getFunctionintroduce();
            if (brief != null) {
                contentMap = contentMap.field("brief", brief);
            }

            String authInfo = gsWxMedia.getAuthenticationInfo();
            if (authInfo != null) {
                contentMap = contentMap.field("authInfo", authInfo);
            }

            if (StringUtils.isNotBlank(tag)) {
                String[] tags = convertTag2Array(tag);
                contentMap = contentMap.field("tags", tags);
            }
            double mediaScore = srcScore + isAuthCent - weixinIDCent - mediaNameLen * mediaNameCent;
            contentMap.field("mediaScore", mediaScore);

            contentMap = contentMap.endObject();
            return new ImmutablePair<>(pmid, contentMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static ImmutablePair<String, XContentBuilder> convert2WxMedia(Document doc, String tag) {
        XContentBuilder contentMap = null;
        double srcScore = 1;
        double mediaNameCent = 0.5;

        String pmid = doc.getString("pmid");
        try {
            if (pmid == null || pmid.length() <= 0) {
                faildNum++;
                return null;
            }

            contentMap = JsonXContent.contentBuilder().startObject();
            contentMap.field("pmid", pmid);

            String mediaName = doc.getString("mediaName");
            int mediaNameLen = 0;
            if (mediaName != null && mediaName.length() > 0) {
                contentMap.field("mediaName", mediaName);
                mediaNameLen = mediaName.length();
            }

            contentMap.field("timeStamp", new Date().getTime());

            Object isAuthString = doc.get("isAuth");
            double isAuthCent = 0;
            if (isAuthString != null) {
                contentMap.field("isAuth", ((Number) isAuthString).intValue());
                isAuthCent = 0.5;
            } else {
                contentMap.field("isAuth", 0);
            }

            String weixinID = doc.getString("weixinID");
            double weixinIDCent = 0;
            if (weixinID != null && weixinID.length() > 0) {
                contentMap.field("weixinID", weixinID);
            } else {
                weixinIDCent = 1;
            }

            String brief = doc.getString("brief");
            if (brief != null && brief.length() > 0) {
                contentMap.field("brief", brief);
            }

            String authInfo = doc.getString("authInfo");
            if (authInfo != null && authInfo.length() > 0) {
                contentMap.field("authInfo", authInfo);
            }

            double mediaScore = srcScore + isAuthCent - weixinIDCent - mediaNameLen * mediaNameCent;
            contentMap.field("mediaScore", mediaScore);

            contentMap.endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new ImmutablePair<>(pmid, contentMap);

    }

    public static ImmutablePair<String, XContentBuilder> convert2WbMedia(Document doc, String tag) {
        XContentBuilder contentMap = null;
        double srcScore = 1;

        String pmid = doc.getString("pmid");
        try {
            if (pmid == null || pmid.length() <= 0) {
                faildNum++;
                return null;
            }
            contentMap = JsonXContent.contentBuilder().startObject();
            contentMap.field("pmid", pmid);

            String mediaName = doc.getString("mediaName");
            if (mediaName != null && mediaName.length() > 0) {
                contentMap.field("mediaName", mediaName);
            }

            contentMap.field("timeStamp", new Date().getTime());

            Object fansNumString = doc.get("fansNum");
            double logFans = 0;
            double fansNumCent = 0;
            if (fansNumString != null) {
                int fansNum = ((Number) fansNumString).intValue();
                contentMap.field("fansNum", fansNum);
                fansNum = fansNum <= 0 ? 1 : fansNum;
                logFans = Math.log(fansNum);
                if (fansNum > BIGFANS) {
                    fansNumCent = 1;
                }
            } else {
                contentMap.field("fansNum", 0);
            }

            Object isAuthString = doc.get("isAuth");
            double isAuthCent = 0;
            if (isAuthString != null) {
                contentMap.field("isAuth", ((Number) isAuthString).intValue());
                isAuthCent = 0.05;
            } else {
                contentMap.field("isAuth", 0);
            }

            String brief = doc.getString("brief");
            if (brief != null && brief.length() > 0) {
                contentMap.field("brief", brief);
            }

            String authInfo = doc.getString("authInfo");
            double authInfoCent = 0;
            if (authInfo != null && authInfo.length() > 0) {
                contentMap.field("authInfo", authInfo);
                authInfoCent = 1;
            }

            double mediaScore = srcScore + fansNumCent + isAuthCent + authInfoCent + logFans;
            contentMap.field("mediaScore", mediaScore);

            contentMap.endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new ImmutablePair<>(pmid, contentMap);
    }

    public static ImmutablePair<String, XContentBuilder> convert2WbMedia(
            WeiboAccount wbMedia, String tag) {
        XContentBuilder contentMap = null;
        double srcScore = 1;
        String pmid = wbMedia.getWeiboID();
        if (pmid == null) {
            return null;
        }
        try {
            contentMap = JsonXContent.contentBuilder().startObject();
            contentMap = contentMap.field("pmid", pmid);

            String mediaName = wbMedia.getNickName();
            if (mediaName != null) {
                contentMap = contentMap.field("mediaName", mediaName);
            }

            contentMap = contentMap.field("timeStamp", new Date().getTime());

            Object fansNumString = wbMedia.getFansNum();
            double logFans = 0;
            double fansNumCent = 0;
            if (fansNumString != null) {
                int fansNum = ((Number) fansNumString).intValue();
                contentMap.field("fansNum", fansNum);
                fansNum = fansNum <= 0 ? 1 : fansNum;
                logFans = Math.log(fansNum);
                if (fansNum > BIGFANS) {
                    fansNumCent = 1;
                }
            } else {
                contentMap.field("fansNum", 0);
            }


            String isAuthString = wbMedia.getVerifiedReason();
            double isAuthCent = 0;
            if (StringUtils.isNotBlank(isAuthString)) {
                contentMap = contentMap.field("isAuth", 1);
                isAuthCent = 0.05;
            } else {
                contentMap = contentMap.field("isAuth", 0);
            }

            String brief = wbMedia.getDesc();
            if (brief != null) {
                contentMap = contentMap.field("brief", brief);
            }

            String authInfo = wbMedia.getVerifiedReason();
            double authInfoCent = 0;
            if (authInfo != null) {
                contentMap = contentMap.field("authInfo", authInfo);
                authInfoCent = 1;
            }
            if (StringUtils.isNotBlank(tag)) {
                String[] tags = convertTag2Array(tag);
                contentMap = contentMap.field("tags", tags);
            }
            double mediaScore = srcScore + fansNumCent + isAuthCent + authInfoCent + logFans;
            contentMap.field("mediaScore", mediaScore);

            contentMap = contentMap.endObject();
            return new ImmutablePair<>(pmid, contentMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Document convert2WxMediaDoc(GsData gsWxMedia, String tag) {
        Document doc = new Document();
        doc.put("brief", gsWxMedia.getFunctionintroduce());
        doc.put("headImage", gsWxMedia.getHeadportrurl());
        doc.put("mediaName", gsWxMedia.getWechatname());
        String pmid = "";
        Matcher matcher = pattern.matcher(gsWxMedia.getIncluded());
        if (matcher.find()) {
            pmid = matcher.group(1);
        }
        if (StringUtils.isNotBlank(pmid)) doc.put("pmid", pmid);
        doc.put("qrcode", gsWxMedia.getQrcodeurl());
        doc.put("weixinID", gsWxMedia.getWechatid());
        if (StringUtils.isNotBlank(gsWxMedia.getAuthenticationInfo())) {
            doc.put("authInfo", gsWxMedia.getAuthenticationInfo());
            doc.put("isAuth", 1);
        }
        if (StringUtils.isNotBlank(tag))
            doc.put("tags", convertTag2Array(tag));
        doc.put("plat", 1);
        return doc;
    }

    public static Document convert2WxMediaDoc(Document doc, String tag) {
        if (StringUtils.isNotBlank(tag))
            doc.put("tags", convertTag2Array(tag));
        doc.put("plat", 1);
        return doc;
    }

    public static Document convert2WbMediaDoc(WeiboAccount wbMedia, String tag) {
        Document doc = new Document();
        if (StringUtils.isNotBlank(wbMedia.getVerifiedReason())) {
            doc.put("authInfo", wbMedia.getVerifiedReason());
            doc.put("isAuth", 1);
        }
        doc.put("brief", wbMedia.getDesc());
        doc.put("headImage", wbMedia.getHeadImg());
        doc.put("location", wbMedia.getActivePlace());
        doc.put("mediaName", wbMedia.getNickName());
        doc.put("pmid", wbMedia.getWeiboID());
        doc.put("fansNum", wbMedia.getFansNum());
        doc.put("postNum", wbMedia.getAttNum());
        if (StringUtils.isNotBlank(tag))
            doc.put("tags", convertTag2Array(tag));
        doc.put("plat", 2);
        return doc;
    }

    public static Document convert2WbMediaDoc(Document doc, String tag) {
        if (StringUtils.isNotBlank(tag))
            doc.put("tags", convertTag2Array(tag));
        doc.put("plat", 2);
        return doc;
    }
}
