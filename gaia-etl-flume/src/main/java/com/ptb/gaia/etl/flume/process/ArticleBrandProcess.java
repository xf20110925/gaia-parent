package com.ptb.gaia.etl.flume.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ptb.gaia.etl.flume.entity.ArticleBrandBean;
import com.ptb.gaia.etl.flume.entity.ArticleBrandStatic;
import com.ptb.gaia.etl.flume.utils.Constant;
import com.ptb.gaia.etl.flume.utils.GaiaConvertUtils;
import com.ptb.gaia.etl.flume.utils.MysqlConnect;
import com.ptb.utils.log.LogUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.ptb.utils.log.LogUtils.ActionResult.failed;
import static com.ptb.utils.log.LogUtils.ActionResult.success;

/**
 * Created by xuefeng on 2016/6/23.
 */
public class ArticleBrandProcess extends ProcessBasic {
    static Logger logger = LoggerFactory.getLogger(ArticleBrandProcess.class);
    private static Map<Long, List<String>> brandKeyWordsMap;
    private static Map<Long, List<String>> categoryKeyWordsMap;
    private static Map<String, Long> categoryDirMap;
    private static Map<String, Long> brandDirMap;
    private static Map<Long, List<Long>> categoryBrandRel;

    public ArticleBrandProcess() throws ConfigurationException {
        super();
    }

    static {
        try {
            brandKeyWordsMap = MysqlConnect.getInstance().queryKeyWords("select t.id,IFNULL(t1.key_words,t.name) as key_words from zeus.article_channel t LEFT JOIN zeus.brand_keyword_brand t1 on t.name=t1.brand_name where t.id is not null ;");
            categoryKeyWordsMap = MysqlConnect.getInstance().queryKeyWords("select t.id,GROUP_CONCAT(t1.key_words) as key_words from zeus.category_brand t join zeus.category_keyword_brand t1 on t.id=t1.category_id group by category_name ;");
            categoryDirMap = MysqlConnect.getInstance().queryDir("select t.category_name,t.id from zeus.category_brand t ;");
            brandDirMap = MysqlConnect.getInstance().queryDir("select t.name,t.id from zeus.article_channel t LEFT JOIN zeus.brand_keyword_brand t1 on t.name=t1.brand_name where t.id is not null ;");
            categoryBrandRel = MysqlConnect.getInstance().queryCategoryBrandRel("select category_id,brand_id from (select t1.category_id,GROUP_CONCAT(t.id) as brand_id from zeus.article_channel t LEFT JOIN zeus.brand_keyword_brand t1 on t.name=t1.brand_name where t.id is not null group by t1.category_id) t5 where t5.category_id is not null ;");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

//    List<Event> events

    @Override
    public void process(List<Event> events) {
        Calendar c = Calendar.getInstance(Locale.CHINA);
        String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.CHINA).format(c.getTimeInMillis());

        events.forEach(event -> {
            try {
                JSONObject jsonObject = (JSONObject) JSON.parse(event.getBody());
                switch (jsonObject.getInteger("plat")) {
                    case Constant.P_WEIXIN:
                        List<ArticleBrandBean> articleBrandBean = GaiaConvertUtils.convertJSONObjectToArticleBrand(jsonObject, brandDirMap, brandKeyWordsMap, categoryDirMap, categoryKeyWordsMap, categoryBrandRel, time);
                        if (!articleBrandBean.get(0).getBrandId().equals(ArticleBrandStatic.Other)) {
                            MysqlConnect.getInstance().insertBatch(articleBrandBean);
                            LogUtils.log("gaia-flume", "process article brand data ", success, "sucess process data num : " + articleBrandBean.size());
                        }
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                logger.info(String.format("error data formate events >> [%s]", new String(event.getBody())), e);
                LogUtils.log("gaia-flume", "article static data formate", failed, JSON.toJSONString(event) + e);
            }
        });
    }


//    public static void main(String[] args) throws ConfigurationException {
//        JSONEvent event = new JSONEvent();
//        event.setBody("{\"content\":\"1908年福特公司推出了福特T型车。从1909年至1913年，福特的T型在多次比赛中获胜。1913年福特退出了比赛因为他对比赛的规则不满。这时候他也没有必要参加比赛了，因为T型已经非常出名。同年福特将流水线引入他的工厂，从而巨大地提高了生产量。1918年半数在美国运行的汽车是T型。福特非常注意倡扬和保护T型的设计。（福特说：“任何顾客可以将这辆车漆成任何他所愿意的颜色，只要它保持它的黑色。”）这个设计一直被保持到1927年。到1927年福特一共生产了1500万T型车。此后45年内这将是一个世界记录。\",\"title\":\"一带一路+新疆振兴”第一龙头，秒杀天山股份，明日全仓目标40%收益！\",\"biz\": \"MjM5OTAzMTE0MA==\",\"url\": \"http://mp.weixin.qq.com/s?__biz=MjM5OTAzMTE0MA==&mid=2654878747&idx=3&sn=5be474b7b055cc8382c280f9885e13f7#rd\",\"plat\":\"1\"}".getBytes());
//        new ArticleBrandProcess().process(Arrays.asList(event));
//
//        JSONEvent event1 = new JSONEvent();
//        event1.setBody("{\"content\":\"马自达（MAZDA），是一家日本汽车制造商，总部设在日本广岛，主要销售市场包括亚洲、欧洲和北美洲。MAZDA是日本最著名的汽车品牌之一，日本第四大汽车制造商，是世界著名汽车品牌，是世界上唯一研发和生产转子发动机的汽车公司。2008年《财富》全球500强企业排名中名列第二百五十五位。\",\"title\":\"一带一路+新疆振兴”第一龙头，秒杀天山股份，明日全仓目标40%收益！\",\"biz\": \"MjM5OTAzMTE0MA==\",\"url\": \"http://mp.weixin.qq.com/s?__biz=MjM5OTAzMTE0MA==&mid=2654878747&idx=3&sn=5be474b7b055cc8382c280f9885e13f7#rd\",\"plat\":\"1\"}".getBytes());
//        new ArticleBrandProcess().process(Arrays.asList(event1));
//
//
//    }

}
