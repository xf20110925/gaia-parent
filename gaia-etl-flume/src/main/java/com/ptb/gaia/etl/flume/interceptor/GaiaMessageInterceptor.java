package com.ptb.gaia.etl.flume.interceptor;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ptb.utils.log.LogUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ptb.utils.log.LogUtils.ActionResult;

import java.util.List;

import static com.ptb.utils.log.LogUtils.ActionResult.failed;
import static javafx.scene.input.KeyCode.L;

/**
 * Created by lishuangbin on 16/3/24.
 */
public class GaiaMessageInterceptor implements Interceptor {

    public static final String MSG_CONTENT = "body";
    public static final String MSG_TYPE = "type";
    public static final String MSG_TYPE_TIMESTAME = "timestamp";

    public static final String EVENT_HEAD_TYPE = "TYPE";
    public static final String EVENT_HEAD_TIMESTAMP = "TIMESTAMP";

    static Logger logger = LoggerFactory.getLogger(GaiaMessageInterceptor.class);

    public void initialize() {

    }

    public Event intercept(Event event) {

        try {
            if(event.getHeaders().containsKey(EVENT_HEAD_TYPE)) {
                return event;
            }
            JSONObject msgBody = JSON.parseObject(event.getBody(), JSONObject.class);
            if (!msgBody.containsKey(MSG_CONTENT)) {
                logger.warn(new String(event.getBody()));
                return null;
            }

            if (!msgBody.containsKey(MSG_TYPE)) {
                logger.warn(new String(event.getBody()));
                return null;
            }

            if (!msgBody.containsKey(MSG_TYPE_TIMESTAME)){
                logger.warn(new String(event.getBody()));
                return null;
            }
            event.setBody(JSON.toJSONBytes(msgBody.get(MSG_CONTENT)));
            event.getHeaders().put(EVENT_HEAD_TYPE, String.valueOf(msgBody.get(MSG_TYPE)));
            event.getHeaders().put(EVENT_HEAD_TIMESTAMP,String.valueOf(msgBody.get(MSG_TYPE_TIMESTAME)));
        } catch (Exception e) {
            logger.warn(JSON.toJSONString(event), e);
            LogUtils.log("gaia-flume", "message interceptor exception", failed, JSON.toJSONString(event) + e);
            return null;
        }

        return event;
    }

    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {
            return new GaiaMessageInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
