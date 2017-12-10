package com.ptb.gaia.etl.flume.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ptb.gaia.etl.flume.utils.Constant;
import com.ptb.gaia.index.EsArticleIndexImp;
import com.ptb.gaia.index.bean.EsInsertData;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.flume.Event;
import org.seleniumhq.jetty7.util.security.Credential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by watson zhang on 2016/11/23.
 */
public class EsArticleProcess extends ProcessBasic{
    private static Logger logger = LoggerFactory.getLogger(EsArticleProcess.class);

    public EsArticleIndexImp esArticleIndexImp = new EsArticleIndexImp();

    public EsArticleProcess() throws ConfigurationException {
    }

    @Override
    public void process(List<Event> events) {
        List<EsInsertData> esInsertWxDatas = new ArrayList<>();
        List<EsInsertData> esInsertWbDatas = new ArrayList<>();
        events.stream().forEach(event -> {
            try {
                JSONObject jsonObject = (JSONObject) JSON.parse(event.getBody());
                int plat = jsonObject.getInteger("plat");
                EsInsertData esInsertData = new EsInsertData();
                if (jsonObject.getString("url") == null){
                    return;
                }
                String urlMd5 = Credential.MD5.digest(jsonObject.getString("url")).substring(4);
                esInsertData.setId(urlMd5);
                esInsertData.setJsonObject(jsonObject);
                switch (plat) {
                    case Constant.P_WEIXIN:
                        esInsertWxDatas.add(esInsertData);
                        break;
                    case Constant.P_WEIBO:
                        esInsertWbDatas.add(esInsertData);
                        break;
                    default:
                        break;
                }
            }catch (Exception e){
                logger.error("handle events error! event:{}",event.getBody(), e);
            }
        });
        if (esInsertWxDatas.size() > 0){
            esArticleIndexImp.upsertWxArticleTitleBatch(esInsertWxDatas);
            esArticleIndexImp.upsertWxArticleContentBatch(esInsertWxDatas);
        }
        if (esInsertWbDatas.size() > 0){
            esArticleIndexImp.upsertWbArticleTitleBatch(esInsertWbDatas);
            esArticleIndexImp.upsertWbArticleContentBatch(esInsertWbDatas);
        }
        return;
    }
}
