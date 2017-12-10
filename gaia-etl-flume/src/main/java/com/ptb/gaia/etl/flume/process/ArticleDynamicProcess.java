package com.ptb.gaia.etl.flume.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ptb.gaia.etl.flume.utils.Constant;
import com.ptb.gaia.etl.flume.utils.GaiaConvertUtils;
import com.ptb.gaia.service.entity.article.GWbArticleDynamic;
import com.ptb.gaia.service.entity.article.GWxArticleDynamic;
import com.ptb.gaia.service.entity.media.GLiveMediaCase;
import com.ptb.utils.log.LogUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.ptb.utils.log.LogUtils.ActionResult.failed;

/**
 * Created by xuefeng on 2016/6/23.
 */
public class ArticleDynamicProcess extends ProcessBasic {
    static Logger logger = LoggerFactory.getLogger(ArticleDynamicProcess.class);

    public ArticleDynamicProcess() throws ConfigurationException {
        super();
    }

    @Override
    public void process(List<Event> events) {
        ArrayList<GWxArticleDynamic> gWxArticleDynamics = new ArrayList<>();
        ArrayList<GWbArticleDynamic> gWbArticleDynamics = new ArrayList<>();
        ArrayList<GLiveMediaCase> gLiveDynamics = new ArrayList<>();
        events.stream().forEach(event -> {
            try {
                JSONObject jsonObject = (JSONObject) JSON.parse(event.getBody());
                switch (jsonObject.getInteger("plat")) {
                    case Constant.P_WEIXIN:
                        GWxArticleDynamic wxArticleDynamic = GaiaConvertUtils.convertJSONObjectToGWxArticleDynamic(jsonObject);
                        gWxArticleDynamics.add(wxArticleDynamic);
                        break;
                    case Constant.P_WEIBO:
                        GWbArticleDynamic wbArticleDynamic = GaiaConvertUtils.convertJSONObjectToGWbArticleDynamic(jsonObject);
                        gWbArticleDynamics.add(wbArticleDynamic);
                        break;
                    case Constant.P_LIVE:
                        GLiveMediaCase gLiveMediaCase = GaiaConvertUtils.convertJsonObjecToGLiveMediaStatic(jsonObject);
                        gLiveDynamics.add(gLiveMediaCase);
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                logger.info(String.format("error data formate events >> [%s]", new String(event.getBody())), e);
                LogUtils.log("gaia-flume", "article dynamic data formate", failed, JSON.toJSONString(event) + e);
            }
        });
        try {
            if (!gWxArticleDynamics.isEmpty()) iGaia.addWxArticleDynamics(gWxArticleDynamics);

        }catch (Exception e) {
            logger.error("plat wx");
            e.printStackTrace();
        }

        try {
            if (!gWbArticleDynamics.isEmpty()) iGaia.addWbArticleDynamics(gWbArticleDynamics);
        }catch (Exception e) {
            logger.error("plat [wb]");
            e.printStackTrace();
        }

        try {
            if (!gLiveDynamics.isEmpty()) iGaia.addLiveArticleDynamicBatch(gLiveDynamics);
        }catch (Exception e) {
            logger.error("plat [live]");
            e.printStackTrace();
        }
    }
}
