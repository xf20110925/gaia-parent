package com.ptb.gaia.index.es;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by watson zhang on 2016/11/24.
 * 所有接口不支持多线程访问
 */
public class MediaConvert {
    private static Logger logger = LoggerFactory.getLogger(MediaConvert.class);
    private static final int BIGFANS = 5000000;

    public Map<String, Object> convertWxMedia(JSONObject jsonObject){
        Map<String, Object> objectMap = this.convertCommonToMap(jsonObject, 1);

        return objectMap;
    }

    public Map<String, Object> convertWbMedia(JSONObject jsonObject){
        Map<String, Object> objectMap = this.convertCommonToMap(jsonObject, 2);
        return objectMap;
    }

    public Map<String, Object> convertCommonToMap(JSONObject jsonObject, int type){
        if (jsonObject == null){
            return null;
        }
        //double baiscScore = 1.0;

        HashedMap objectMap = new HashedMap();

        int isAuth = 0;
        if (type == 1){
            if (StringUtils.isNotBlank(jsonObject.getString("biz"))){
                objectMap.put("pmid", jsonObject.getString("biz"));
            }
            if (StringUtils.isNotBlank(jsonObject.getString("authentication"))){
                isAuth = 1;
                objectMap.put("authType", isAuth);
            }
            String weixinID = jsonObject.getString("weixinId");
            if(StringUtils.isNotBlank(weixinID) && weixinID.length() > 0){
                objectMap.put("weixinId", weixinID);
            }

            int pi = 0;
            if (jsonObject.getInteger("pi") != null){
                pi = jsonObject.getInteger("pi");
                objectMap.put("pi", pi);
            }

        }else if (type == 2){
            if (StringUtils.isNotBlank(jsonObject.getString("weiboId"))){
                objectMap.put("pmid", jsonObject.getString("weiboId"));
            }

            String authType = jsonObject.getString("authType");
            String authDescription = jsonObject.getString("authDescription");
            if(StringUtils.isBlank(authDescription)) {
                authType = "";
            }else {
                objectMap.put("authDesc", 1);
            }
            switch (authType) {
                case "":
                case "-1":
                    isAuth = 0;
                    break;
                case "0":
                    isAuth = 1;
                    break;
                default:
                    isAuth = 2;
                    break;
            }
            if (StringUtils.isNotBlank(authType)){
                objectMap.put("authType", isAuth);
            }

            Object fansNumString = jsonObject.get("fans");
            if(fansNumString != null){
                int fansNum = ((Number)fansNumString).intValue();
                objectMap.put("fansNum", fansNum);
            }
        }

        String mediaName = jsonObject.getString("mediaName");
        if(StringUtils.isNotBlank(mediaName) && mediaName.length() > 0){
            objectMap.put("mediaName", mediaName);
            objectMap.put("mediaNameLen", mediaName.length());
        }

        //objectMap.put("preScore", baiscScore);

        return objectMap;
    }

    public double getMediaScore(int type, JSONObject oldJson, Map<String, Object> objectMap){
        double baseScore = 1.0;

        if (objectMap.containsKey("authType")) {
            if ((Integer)objectMap.get("authType") > 0)
                baseScore += 0.05;
        }else if (oldJson != null && oldJson.getInteger("authType") != null){
            if (oldJson.getInteger("authType") > 0)
                baseScore += 0.05;
        }

        if (objectMap.containsKey("authDesc")) {
            if ((Integer)objectMap.get("authDesc") > 0)
                baseScore += 0.05;
        }else if (oldJson != null && oldJson.getInteger("authDesc") != null){
            if (oldJson.getInteger("authDesc") > 0)
                baseScore += 0.05;
        }

        if (type == 1){
            if (objectMap.containsKey("weixinId")){
                baseScore += 1;
            }else if (oldJson != null && oldJson.getString("weixinId") != null){
                baseScore += 1;
            }
        }else if (type ==2){
            Integer fansNum = 0;
            if (objectMap.containsKey("fansNum")){
                fansNum = (Integer) objectMap.get("fansNum");
            }else if (oldJson != null && oldJson.getInteger("fansNum") != null){
                fansNum = oldJson.getInteger("fansNum");
            }
            baseScore += Math.log(fansNum <= 0? 1:fansNum);
            if(fansNum > BIGFANS) {
                baseScore += 1.0;
            }
        }

        return baseScore;
    }

    public double getMediaScore(int type,  Map<String, Object> json){
        double baseScore = 1.0;

        if (json.containsKey("authType")) {
            if ((Integer)json.get("authType") > 0)
                baseScore += 0.05;
        }
        if (json.containsKey("authDesc")) {
            if ((Integer)json.get("authDesc") > 0)
                baseScore += 0.05;
        }
        if (type == 1){
            if (json.containsKey("weixinId")){
                baseScore += 1;
            }
        }else if (type ==2){
            Integer fansNum = 0;
            if (json.containsKey("fansNum")){
                fansNum = (Integer) json.get("fansNum");
            }
            baseScore += Math.log(fansNum <= 0? 1:fansNum);
            if(fansNum > BIGFANS) {
                baseScore += 1.0;
            }
        }
        return baseScore;
    }

    public Map<String, Object> mongoConvertToEs(JSONObject jsonObject, int mediaType){
        double srcScore = 1.0;

        String pmid = jsonObject.getString("pmid");
        if(pmid == null || pmid.length() <= 0){
            return null;
        }

        HashedMap esObjectMap;
        String mediaName = jsonObject.getString("mediaName");
        if(mediaName != null && mediaName.length() > 0){
            esObjectMap = new HashedMap();
            esObjectMap.put("pmid", pmid);
            esObjectMap.put("mediaName", mediaName);
        } else {
            return null;
        }

        Object isAuth = jsonObject.get("isAuth");
        double isAuthCent = 0;
        if(isAuth != null){
            int isAuthNum = ((Number)isAuth).intValue();
            if (isAuthNum > 0){
                isAuthCent = 0.5;
            }
        }

        if (mediaType == 1){

            Object pi = jsonObject.get("pi");
            int piNum = 0;
            if (pi != null){
                piNum = ((Number)pi).intValue();
                esObjectMap.put("pi", piNum);
            }else {
                esObjectMap.put("pi", piNum);
            }

            int mediaNameLen = mediaName.length();
            esObjectMap.put("mediaNameLen", mediaNameLen);

            String weixinID = jsonObject.getString("weixinId");
            double weixinIDCent = 0;
            if(weixinID != null && weixinID.length() > 0){
            } else {
                weixinIDCent = 1;
            }

            double mediaScore = srcScore + piNum/500.0 + isAuthCent - weixinIDCent;
            esObjectMap.put("mediaScore", mediaScore);
        }else if (mediaType == 2){
            Object fansNumString = jsonObject.get("fansNum");
            double logFans = 0;
            double fansNumCent = 0;
            if(fansNumString != null){
                int fansNum = ((Number)fansNumString).intValue();
                fansNum = fansNum <= 0? 1:fansNum;
                logFans = Math.log(fansNum);
                if(fansNum > BIGFANS) {
                    fansNumCent = 1;
                }
            }
            String authInfo = jsonObject.getString("authInfo");
            double authInfoCent = 0;
            if(authInfo != null && authInfo.length() > 0){
                authInfoCent = 0.03;
            }

            double mediaScore = srcScore + fansNumCent + isAuthCent + authInfoCent + logFans;
            esObjectMap.put("mediaScore", mediaScore);
        }
        return esObjectMap;
    }

    public static void main(String[] args){
        MediaConvert mediaConvert = new MediaConvert();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("biz", "adfas");
        jsonObject.put("weixinId", "afafs");
        mediaConvert.convertCommonToMap(jsonObject, 1);
    }
}
