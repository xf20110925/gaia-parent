package com.ptb.gaia.etl.flume.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ptb.gaia.etl.flume.utils.Constant;
import com.ptb.gaia.index.bean.EsInsertData;
import com.ptb.gaia.index.EsMediaIndexImp;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by watson zhang on 2016/11/23.
 */
public class EsMediaProcess extends ProcessBasic{
    private static Logger logger = LoggerFactory.getLogger(EsMediaProcess.class);

    public EsMediaIndexImp esMediaIndexImp = new EsMediaIndexImp();

    public EsMediaProcess() throws ConfigurationException {
    }

    @Override
    public void process(List<Event> events) {
        if (events == null){
            return;
        }
        List<EsInsertData> esInsertWxDatas = new ArrayList<>();
        List<EsInsertData> esInsertWbDatas = new ArrayList<>();
        events.stream().forEach(event -> {
            try {
                JSONObject jsonObject = (JSONObject) JSON.parse(event.getBody());
                int plat = jsonObject.getInteger("plat");
                EsInsertData esInsertData = new EsInsertData();
                esInsertData.setJsonObject(jsonObject);
                switch (plat) {
                    case Constant.P_WEIXIN:
                        if (jsonObject.getString("biz") == null){
                            logger.error("no biz Id!");
                            break;
                        }
                        esInsertData.setId(jsonObject.getString("biz"));
                        esInsertWxDatas.add(esInsertData);
                        break;
                    case Constant.P_WEIBO:
                        if (jsonObject.getString("weiboId") == null){
                            logger.error("no weibo Id!");
                            break;
                        }
                        esInsertData.setId(jsonObject.getString("weiboId"));
                        esInsertWbDatas.add(esInsertData);
                        break;
                    case Constant.P_LIVE:
                         if (jsonObject.getString("pmid") == null){
                            logger.error("no weibo Id!");
                            break;
                        }
                        esInsertData.setId(jsonObject.getString("pmid"));
                        esInsertWbDatas.add(esInsertData);
                        esMediaIndexImp.insertLiveMediaFromKafka(esInsertData);
                        break;
                    default:
                        break;
                }
            }catch (Exception e){
                logger.error("handle events error! event:{}",event.getBody(), e);
            }
        });
        if (esInsertWxDatas.size() > 0){
            esMediaIndexImp.upsertWxMediaBatch(esInsertWxDatas);
        }
        if (esInsertWbDatas.size() > 0){
            esMediaIndexImp.upsertWbMediaBatch(esInsertWbDatas);
        }
    }
}
