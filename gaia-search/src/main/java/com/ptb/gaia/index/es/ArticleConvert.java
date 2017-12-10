package com.ptb.gaia.index.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.map.HashedMap;
import org.bson.Document;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by watson zhang on 2016/11/24.
 */
public class ArticleConvert {
    private static Logger logger = LoggerFactory.getLogger(ArticleConvert.class);

    /**
     * 从kafka读取数据并转换成ES类型
     * @param doc
     * @param type
     * @return
     */
    public Map<String, Object> convertDocToTitleObject(JSONObject doc, int type){
        if (doc == null){
            logger.error("[ERROR]title doc is null!");
            return null;
        }
        Map<String, Object> contentBuilder = this.covertCommonToMap(doc, type);
        if (contentBuilder == null){
            return null;
        }
        String title = doc.getString("title");
        if (title != null){
            contentBuilder.put("title", title);
        }
        return contentBuilder;
    }

    /**
     * 从kafka读取数据并转换成ES类型
     * @param doc
     * @param type
     * @return
     */
    public Map<String, Object> convertDocToContentObject(JSONObject doc, int type){
        if (doc == null){
            logger.error("[ERROR]content doc is null!");
            return null;
        }
            Map<String, Object> contentBuilder = this.covertCommonToMap(doc, type);
            if (contentBuilder == null){
                return null;
            }
            String content = doc.getString("content");
            if (content != null){
                String text = Jsoup.parseBodyFragment(content).text();
                if (text != null){
                    contentBuilder.put("content", text);
                }
            }
            return contentBuilder;
    }


    /**
     * 从kafka读取数据并转换成ES类型
     * @param doc
     * @param type
     * @return
     */
    public Map<String, Object> covertCommonToMap(JSONObject doc, int type){
        double relRate = 1.0;
        if (doc == null){
            logger.error("[ERROR]doc is null!");
            return null;
        }

        Map<String, Object> objectMap = new HashedMap();

        int readIntNum = this.getGradeNum(doc, type);
        if (readIntNum != 0){
            if (type == 1){
                objectMap.put("readNum", readIntNum);
            }else if (type == 2){
                objectMap.put("zpzNum", readIntNum);
            }
            if (readIntNum <= 0){
                readIntNum = 1;
            }
            relRate += Math.log(readIntNum);
            if (readIntNum > 100000){
                relRate += 1;
            }
        }

        //likeNum 权重设置
        int likeIntNum = 0;
        if (doc.getInteger("likes") != null){
            likeIntNum = doc.getInteger("likes");
            //relRate += likeIntNum/10000.0;
            objectMap.put("likes", likeIntNum);
        }

        //double origScore = 0.5;
        int isOriginal = 0;
        if (doc.getBoolean("original") != null){
            if (doc.getBoolean("original").equals(true)){
                isOriginal = 1;
            }
            objectMap.put("isOriginal", isOriginal);
        }

        if (type == 1){
            if (doc.getString("biz") != null){
                objectMap.put("pmid", doc.getString("biz"));
            }
        }else if (type == 2){
            if (doc.getString("weiboId") != null){
                objectMap.put("pmid", doc.getString("weiboId"));
            }
        }

        if (doc.getLong("postTime") != null){
            long postTime = doc.getLong("postTime") * 1000;
            objectMap.put("postTime", postTime);
        }

        //objectMap.put("preScore", relRate);

        if (doc.getString("url") != null){
            objectMap.put("articleUrl", doc.getString("url"));
        }
        return objectMap;
    }

    /**
     * upsert 方式索引文章时计算分数
     * @param type
     * @param oldJson
     * @param objectMap
     * @return
     */
    public double getArticleScore(int type, JSONObject oldJson, Map<String, Object> objectMap){
        double baseScore = 1.0;

        try {
            if (objectMap.containsKey("isOriginal")) {
                if ((Integer)objectMap.get("isOriginal") > 0)
                    baseScore += 0.5;
            }else if (oldJson != null && oldJson.getInteger("isOriginal") != null){
                if (oldJson.getInteger("isOriginal") > 0)
                    baseScore += 0.5;
            }

            if (objectMap.containsKey("readNum")) {
                int readNum = Integer.parseInt(objectMap.get("readNum").toString());
                if (readNum > 0)
                    baseScore += readNum / 100001.0;
            }else if (oldJson != null && oldJson.getInteger("readNum") != null){
                if (oldJson.getInteger("readNum") > 0)
                    baseScore += oldJson.getInteger("readNum")/100001.0;
            }

            if (objectMap.containsKey("likes")) {
                if ((Integer)objectMap.get("likes") > 0)
                    baseScore += 0.05;
            }else if (oldJson != null && oldJson.getInteger("likes") != null){
                if (oldJson.getInteger("likes") > 0)
                    baseScore += 0.05;
            }

        }catch (NumberFormatException e){
            logger.error("read num is error!", e);
        }

        return baseScore;
    }

    /**
     * 直接insert方式计算分数
     * @param type
     * @param objectMap
     * @return
     */
    public double getArticleScore(int type, Map<String, Object> objectMap){
        double baseScore = 1.0;
        try {
            if (objectMap.containsKey("isOriginal")) {
                if ((Integer)objectMap.get("isOriginal") > 0)
                    baseScore += 0.5;
            }
            if (objectMap.containsKey("readNum")) {
                int readNum = Integer.parseInt(objectMap.get("readNum").toString());
                if (readNum > 0)
                    baseScore += readNum / 100001.0;
            }

            if (objectMap.containsKey("likes")) {
                if ((Integer)objectMap.get("likes") > 0)
                    baseScore += 0.05;
            }

        }catch (NumberFormatException e){
            logger.error("read num is error!", e);
        }
        return baseScore;
    }

    public int getGradeNum(JSONObject jsonObject, int type){
        int gradeNum = 0;
        if (type == 1){
            if (jsonObject.getInteger("reads") != null){
                gradeNum = jsonObject.getInteger("reads");
            }
        } else if (type == 2){
            if (jsonObject.getInteger("likes") != null){
                gradeNum += jsonObject.getInteger("likes");
            }
            if (jsonObject.getInteger("comments") != null){
                gradeNum += jsonObject.getInteger("comments");
            }
            if (jsonObject.getInteger("forwards") != null){
                gradeNum += jsonObject.getInteger("forwards");
            }
        }else {
            logger.error("type error type:{}", type);
            return -1;
        }
        return gradeNum;
    }

    /**
     * 从mongo中导入ES时使用下面连个结构,因为flume中的属性名字与mongo中不同
     * @param doc
     * @param type
     * @return
     */
    public HashedMap mongoToEsTitle(JSONObject doc, int type){
        double relRate = 1.0;

        int readIntNum = 0;
        if (type == 1){
            if (doc.getInteger("readNum") != null){
                readIntNum = Integer.valueOf(doc.getInteger("readNum"));
            }
        }else if (type == 2){
            if (doc.getInteger("zpzNum") != null){
                readIntNum = Integer.valueOf(doc.getInteger("zpzNum"));
            }
        }
        //relRate += readIntNum/100001.0;

        //暂时文章搜索不需要评分功能
        //double origScore = 0.5;
        //if (doc.getInteger("isOriginal") != null && doc.getInteger("isOriginal").equals(1)){
        //    relRate += origScore;
        //}else {
        //    relRate -= origScore;
        //}
        HashedMap hashedMap;

        String title = doc.getString("title");
        if (title != null){
            hashedMap = new HashedMap();
            hashedMap.put("title", title);
        }else {
            return null;
        }

        if (doc.getInteger("readNum") != null){
            hashedMap.put("readNum", doc.getInteger("readNum"));
        }

        if (doc.getInteger("zpzNum") != null){
            hashedMap.put("zpzNum", doc.getInteger("zpzNum"));
        }

        if (doc.getInteger("isOriginal") != null){
            hashedMap.put("isOriginal", doc.getInteger("isOriginal"));
        }

        if (doc.getString("pmid") != null){
            hashedMap.put("pmid", doc.getString("pmid"));
        }

        if (doc.getString("postTime") != null){
            String postTime = doc.getString("postTime");
            JSONObject jsonObject = JSON.parseObject(postTime);
            hashedMap.put("postTime", jsonObject.getLong("$numberLong"));
        }

        //hashedMap.put("preScore", relRate);

        if (doc.getString("articleUrl") != null){
            hashedMap.put("articleUrl", doc.getString("articleUrl"));
        }
        return hashedMap;
    }

    /**
     * @param doc
     * @param type
     * @return
     */
    public HashedMap mongoToEsContent(JSONObject doc, int type){
        double relRate = 1.0;

        int readIntNum = 0;
        if (type == 1){
            if (doc.getInteger("readNum") != null){
                readIntNum = Integer.valueOf(doc.getInteger("readNum"));
            }
        }else if (type == 2){
            if (doc.getString("zpzNum") != null){
                readIntNum = Integer.valueOf(doc.getInteger("zpzNum"));
            }
        }
        //relRate += readIntNum/100001.0;

        //暂时文章搜索不需要评分功能
        //double origScore = 0.5;
        //if (doc.getInteger("isOriginal") != null && doc.getInteger("isOriginal").equals(1)){
        //    relRate += origScore;
        //}else {
        //    relRate -= origScore;
        //}
        HashedMap hashedMap;
        if (doc.getString("content") != null){
            hashedMap = new HashedMap();
            hashedMap.put("content", doc.getString("content"));
        }else {
            //logger.info("there are no content! {}", doc.getString("articleUrl"));
            return null;
        }
        if (doc.getInteger("readNum") != null){
            hashedMap.put("readNum", doc.getInteger("readNum"));
        }

        if (doc.getInteger("zpzNum") != null){
            hashedMap.put("zpzNum", doc.getInteger("zpzNum"));
        }

        if (doc.getInteger("isOriginal") != null){
            hashedMap.put("isOriginal", doc.getInteger("isOriginal"));
        }

        if (doc.getString("pmid") != null){
            hashedMap.put("pmid", doc.getString("pmid"));
        }

        if (doc.getString("postTime") != null){
            String postTime = doc.getString("postTime");
            JSONObject jsonObject = JSON.parseObject(postTime);
            hashedMap.put("postTime", jsonObject.getLong("$numberLong"));
        }

        //if (doc.getInteger("articleType") != null){
        //    hashedMap.put("articleType", doc.getInteger("articleType"));
        //}

        //hashedMap.put("preScore", relRate);

        if (doc.getString("articleUrl") != null){
            hashedMap.put("articleUrl", doc.getString("articleUrl"));
        }
        return hashedMap;
    }
}
