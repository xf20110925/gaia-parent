package com.ptb.gaia.index.es;

import com.alibaba.fastjson.JSONObject;
import com.ptb.gaia.index.bean.LiveReqMediaIndex;
import com.ptb.gaia.index.bean.ReqMediaBasicIndex;
import com.ptb.gaia.index.bean.WbReqMediaIndex;
import com.ptb.gaia.index.bean.WxReqMediaIndex;
import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by watson zhang on 2017/3/24.
 */
public class WebMediaSearchConvert {
    private static Logger logger = LoggerFactory.getLogger(WebMediaSearchConvert.class);

    public Map<String, Object> convertToEs(JSONObject jsonObject){
        Map<String, Object> esMap = new HashedMap();


        String mediaName = jsonObject.getString("productName");
        if(mediaName != null && mediaName.length() > 0){
            esMap = new HashedMap();
            esMap.put("productName", mediaName);
        } else {
            logger.warn("no productName");
            return null;
        }

        Object platIcon = jsonObject.get("productId");
        if (platIcon != null){
            esMap.put("productId", Long.parseLong(platIcon.toString()));
        }else {
            logger.warn("no productId");
            return null;
        }

        Object fansNum = jsonObject.get("status");
        if (fansNum != null){
            esMap.put("status", Integer.parseInt(fansNum.toString()));
        }

        String pmid = jsonObject.getString("pmid");
        if (pmid != null){
            esMap.put("pmid", pmid);
        }

        Object mediaType = jsonObject.get("mediaType");
        if (mediaType != null){
            esMap.put("mediaType", Integer.parseInt(mediaType.toString()));
        }

        Object price = jsonObject.get("price");
        if (price != null){
            esMap.put("price", Long.parseLong(price.toString()));
        }

        esMap.put("preScore", 0.0);
        return esMap;
    }

    private void convertMediaBasicToEs(Map<String, Object> esMap, ReqMediaBasicIndex reqMediaBasicIndex){
        esMap.put("mediaName", reqMediaBasicIndex.getMediaName());
        esMap.put("platType", reqMediaBasicIndex.getPlatType());
        esMap.put("mediaType", reqMediaBasicIndex.getMediaType());
        esMap.put("pmid", reqMediaBasicIndex.getPmid());
        esMap.put("price", reqMediaBasicIndex.getPrice());
        esMap.put("agentNum", reqMediaBasicIndex.getAgentNum());
        esMap.put("isAuth", reqMediaBasicIndex.getIsAuth());
    }

    public Map<String, Object> convertWxToEs(WxReqMediaIndex wxMediaReq){
        Map<String, Object> esMap = new HashedMap();
        this.convertMediaBasicToEs(esMap, wxMediaReq);

        esMap.put("iId", wxMediaReq.getiIds());
        esMap.put("hlavgRead", wxMediaReq.getHlavgRead());
        esMap.put("hlavgZan", wxMediaReq.getHlavgZan());
        esMap.put("isOriginal", wxMediaReq.getIsOriginal());
        esMap.put("wxId", wxMediaReq.getWxId());
        esMap.put("pi", wxMediaReq.getPi());

        double wxScore = 0;
        if (wxMediaReq.getWxId() == null || wxMediaReq.getWxId().length() <= 0){
            wxScore = 0.9;
        }
        double isAuth = 0;
        if (wxMediaReq.getIsAuth() == 0){
            isAuth = 0.5;
        }

        double mediaScore = 1.0 + wxMediaReq.getPi()/500.0 + isAuth - wxScore;
        esMap.put("mediaScore", mediaScore);
        return esMap;
    }

    public Map<String, Object> convertWbToEs(WbReqMediaIndex wbMediaReq){
        Map<String, Object> esMap = new HashedMap();
        this.convertMediaBasicToEs(esMap, wbMediaReq);

        esMap.put("iId", wbMediaReq.getiIds());
        esMap.put("fansNum", wbMediaReq.getFansNum());
        esMap.put("avgLike", wbMediaReq.getAvgLike());
        esMap.put("avgSpread", wbMediaReq.getAvgSpread());
        esMap.put("avgComment", wbMediaReq.getAvgComment());
        int fansNum = wbMediaReq.getFansNum() <= 0? 1:wbMediaReq.getFansNum();
        double logFans = Math.log(fansNum);
        double isAuth = wbMediaReq.getIsAuth() == 0? 0:0.5;
        double fansScore = 0;
        if (wbMediaReq.getFansNum() >= 1000000){
            fansScore = 1.0;
        }else if (wbMediaReq.getFansNum() >= 5000000) {
            fansScore = 2.0;
        }else if (wbMediaReq.getFansNum() >= 10000000){
            fansScore = 3.0;
        }else if (wbMediaReq.getFansNum() >= 20000000){
            fansScore = 5.0;
        }
        double authInfoCent = 0;
        if (wbMediaReq.isIfBrief()){
            authInfoCent = 0.03;
        }
        double mediaScore = 1.0 + logFans + isAuth + fansScore + authInfoCent;
        esMap.put("mediaScore", mediaScore);
        return esMap;
    }

    public Map<String, Object> convertLiveToEs(LiveReqMediaIndex liveMediaReq){
        Map<String, Object> esMap = new HashedMap();
        this.convertMediaBasicToEs(esMap, liveMediaReq);

        esMap.put("fansNum", liveMediaReq.getFansNum());
        esMap.put("location", liveMediaReq.getLocation());
        esMap.put("avgOnlineNum", liveMediaReq.getAvgOnlineNum());
        esMap.put("gender", liveMediaReq.getGender());
        esMap.put("mediaScore", 0);

        return esMap;
    }

}
