package com.ptb.gaia.etl.flume.interceptor;

import com.ptb.utils.log.LogUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static com.ptb.utils.log.LogUtils.ActionResult.failed;

/**
 * Created by MengChen on 2016/7/29.
 */
public class GaiaNginxLogAnalysisInterceptor implements Interceptor {

    static Logger logger = LoggerFactory.getLogger(GaiaNginxLogAnalysisInterceptor.class);

    @Override
    public void initialize() {

    }


    public String timeToTimetemp(String time) throws ParseException {

        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.ENGLISH);

        return String.valueOf(formatter.parse(time).getTime());

    }



    @Override
    public Event intercept(Event event) {

        if (event == null || event.getBody() == null || event.getBody().length == 0) {

            return null;

        }

        try {
            String[] eventStr = new String(event.getBody(), "UTF-8").split(":::");
            Collections.replaceAll(Arrays.asList(eventStr), "-", "null");
            eventStr[0] = timeToTimetemp(eventStr[0]);
            event.getHeaders().put("bustype", eventStr[4].toUpperCase());
            event.setBody(String.join("\001", eventStr).getBytes());

        } catch (Exception e) {
            try {
                logger.warn(new String(event.getBody(), "UTF-8"), e);
                LogUtils.log("gaia-log-flume", "message interceptor exception", failed, new String(event.getBody()) + e);
            } catch (UnsupportedEncodingException e1) {
                e1.printStackTrace();
            }

        }

        return event;


    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {
            return new GaiaNginxLogAnalysisInterceptor();
        }

        public void configure(Context context) {

        }
    }

}
