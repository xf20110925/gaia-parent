package com.ptb.gaia.index.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * Created by watson zhang on 2016/11/25.
 */
public class EsInsertData {

    String id;

    JSONObject jsonObject;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public JSONObject getJsonObject() {
        return jsonObject;
    }

    public void setJsonObject(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }
}
