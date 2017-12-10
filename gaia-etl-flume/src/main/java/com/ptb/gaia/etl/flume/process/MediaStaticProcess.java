package com.ptb.gaia.etl.flume.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ptb.gaia.etl.flume.utils.Constant;
import com.ptb.gaia.etl.flume.utils.GaiaConvertUtils;
import com.ptb.gaia.service.IGaia;
import com.ptb.gaia.service.IGaiaImpl;
import com.ptb.gaia.service.entity.media.GVedioMediaStatic;
import com.ptb.gaia.service.entity.media.GWbMediaBasic;
import com.ptb.gaia.service.entity.media.GWxMediaBasic;
import com.ptb.gaia.service.entity.media.GLiveMediaStatic;
import com.ptb.utils.log.LogUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

import static com.ptb.utils.log.LogUtils.ActionResult.failed;

/**
 * Created by eric on 16/6/23.
 */

public class MediaStaticProcess extends ProcessBasic {
    static Logger logger = LoggerFactory.getLogger(MediaStaticProcess.class);
    IGaia iGaia = new IGaiaImpl();


    public MediaStaticProcess() throws ConfigurationException {
        super();
    }

    @Override
    public void process(List<Event> events) {
        List<GWxMediaBasic> gWxMediaBasics = new LinkedList<>();
        List<GWbMediaBasic> gWbMediaBasics = new LinkedList<>();
        List<GLiveMediaStatic> gLiveMediaStatics = new LinkedList<>();
        List<GVedioMediaStatic> gVedioMediaStatics = new LinkedList<>();
        events.stream().forEach(event -> {
            try {

                JSONObject jsonObject = (JSONObject) JSON.parse(event.getBody());
                switch (jsonObject.getInteger("plat")) {
                    case Constant.P_WEIXIN:
                        gWxMediaBasics.add(GaiaConvertUtils.convertJSONObjectToGWxMediaBasic(jsonObject));
                        break;
                    case Constant.P_WEIBO:
                        gWbMediaBasics.add(GaiaConvertUtils.convertJSONObjectToGWbMediaBasic(jsonObject));
                        break;
                    case Constant.P_LIVE:
                        gLiveMediaStatics.add(GaiaConvertUtils.convertJSONobjectLiveMediaStatic(jsonObject));
                        break;
                    case Constant.P_VEDIO:
                        gVedioMediaStatics.add(GaiaConvertUtils.convertJSONobjectVedioMediaStatic(jsonObject));
                        break;

                }
            } catch (Exception e) {
                logger.info(String.format("error data formate events >> [%s]", new String(event.getBody())), e);
                LogUtils.log("gaia-flume", "media static data formate", failed, JSON.toJSONString(event) + e);
            }
        });
        if (gWbMediaBasics.size() > 0) {
            iGaia.addWbMediaBasicBatch(gWbMediaBasics);
        }
        if (gWxMediaBasics.size() > 0) {
            iGaia.addWxMediaBasicBatch(gWxMediaBasics);
        }
        if (gLiveMediaStatics.size() > 0){
            iGaia.addLiveMediaBasicBatch(gLiveMediaStatics);
        }
        if (gVedioMediaStatics.size() > 0){
            iGaia.addVedioMediaBasicBatch(gVedioMediaStatics);
        }
    }
}
