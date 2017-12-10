package com.ptb.gaia.index.es;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by watson zhang on 2017/3/20.
 */
public class LiveMediaConvert {
    private static Logger logger = LoggerFactory.getLogger(LiveMediaConvert.class);
    public Map<String, Object> kafkaConvertToEs(JSONObject jsonObject){
        Map<String, Object> esMap;

        String mediaName = jsonObject.getString("mediaName");
        if(mediaName != null && mediaName.length() > 0){
            esMap = new HashedMap();
            esMap.put("mediaName", mediaName);
        } else {
            logger.warn("no mediaName");
            return null;
        }

        Object platIcon = jsonObject.get("platIcon");
        if (platIcon != null){
            esMap.put("platIcon", Integer.parseInt(platIcon.toString()));
        }else {
            logger.warn("no platIcon");
            return null;
        }

        Object fansNum = jsonObject.get("fans");
        if (fansNum != null){
            esMap.put("fansNum", Integer.parseInt(fansNum.toString()));
        }

        String pmid = jsonObject.getString("pmid");
        if (pmid != null){
            esMap.put("pmid", pmid);
        }

        Object isVip = jsonObject.get("isVip");
        if (isVip != null){
            esMap.put("isVip", Integer.parseInt(isVip.toString()));
        }

        String location = jsonObject.getString("location");
        if (location != null){
            esMap.put("location", location);
        }

        Object gender = jsonObject.get("gender");
        if (gender != null){
            esMap.put("gender", Integer.parseInt(gender.toString()));
        }
        return esMap;
    }


    public Map<String, Object> mongoConvertToEs(JSONObject jsonObject){

        Map<String, Object> esMap;

        String mediaName = jsonObject.getString("mediaName");
        if(mediaName != null && mediaName.length() > 0){
            esMap = new HashedMap();
            esMap.put("mediaName", mediaName);
        } else {
            return null;
        }

        Object platIcon = jsonObject.get("platIcon");
        if (platIcon != null){
            esMap.put("platIcon", Integer.parseInt(platIcon.toString()));
        }

        Object fansNum = jsonObject.get("fansNum");
        if (fansNum != null){
            esMap.put("fansNum", Integer.parseInt(fansNum.toString()));
        }

        String id = jsonObject.getString("pmid");
        esMap.put("pmid", id);

        Object isVip = jsonObject.get("isVip");
        if (isVip != null){
            esMap.put("isVip", Integer.parseInt(isVip.toString()));
        }

        String location = jsonObject.getString("location");
        if (location != null){
            esMap.put("location", location);
        }

        Object gender = jsonObject.get("gender");
        if (gender != null){
            esMap.put("gender", Integer.parseInt(gender.toString()));
        }

        return esMap;
    }


}
