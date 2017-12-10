package com.ptb.gaia.tool.command;

/**
 * @DESC:
 * @VERSION:
 * @author: xuefeng
 * @Date: 2016/7/18
 * @Time: 11:08
 */

import com.alibaba.fastjson.JSON;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.ptb.gaia.service.utils.MongoUtils;
import com.ptb.gaia.tool.esTool.util.ConvertUtils;
import com.ptb.uranus.common.entity.CollectType;
import com.ptb.uranus.schedule.model.Priority;
import com.ptb.uranus.schedule.trigger.JustOneTrigger;
import com.ptb.uranus.sdk.UranusSdk;
import com.ptb.uranus.spider.weibo.WeiboSpider;
import com.ptb.uranus.spider.weibo.bean.WeiboAccount;
import com.ptb.uranus.spider.weixin.bean.GsData;
import com.ptb.uranus.spider.weixin.parse.GsDataWeixinParser;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TagCommand {
    static String host;
    static String port;
    static String dbName;
    static String indexName;
    static List<String> failList = new LinkedList<>();

    static {
        try {
            Configuration conf = new PropertiesConfiguration("athene.properties");
            host = conf.getString("gaia.mongo.host", "192.168.5.31");
            port = conf.getString("gaia.mongo.port", "27017");
            dbName = conf.getString("gaia.mongo.dbName", "test");
            MongoUtils.configure(host, Integer.parseInt(port));
            indexName = conf.getString("athene.es.indexName", "media_c");
        } catch (ConfigurationException e) {
        }
    }

    static WeiboSpider wbSpider = new WeiboSpider();
    static Map<Pattern, Integer> wbPatternMap = getWbPatternMap();

    public static void start(String datePath, String retPath) {
        try {
            Path path1 = Paths.get(datePath);
            BufferedReader bufferedReader = Files.newBufferedReader(path1, Charset.forName("GBK"));
            bufferedReader.lines().skip(1).parallel().forEach(line -> {
                try {
                    if (line.contains("t.sina.com.cn")) {
                        line = line.replace("t.sina.com.cn", "weibo.com/u");
                    }
                    System.out.println("正在添加媒体-》" + line);
                    String[] split = line.split(",");
                    if (split[0].equals("1")) {
                        handleWxMedia(split[2], split[1], retPath);
                    } else {
                        handleWbMedia(split[2], split[1], retPath);
                    }
                } catch (Exception e) {
                    System.out.println("失败的媒体数据-》" + line);
                    failList.add(line);
                }
            });
            System.out.println(String.format("处理成功条数-》%d,失败媒体%s", counter.get(), failList.toString()));
        } catch (IOException e) {
            throw new RuntimeException(String.format("fail load file %s", datePath));
        }
    }

    private static String getStrByPatterns(String content, Map<Pattern, Integer> patterns) {
        String userId = null;
        for (Map.Entry<Pattern, Integer> entry : patterns.entrySet()) {
            Matcher matcher = entry.getKey().matcher(content);
            if (matcher.find()) {
                userId = matcher.group(entry.getValue());
                return userId;
            }
        }
        return userId;
    }

    private static Map<Pattern, Integer> getWbPatternMap() {
        String userIdRegex1 = "http://(www\\.)?weibo.com/(\\d+)/profile.*";
        String userIdRegex2 = "http://(www\\.|e\\.)?weibo.com/u/(\\d+).*";
        String userIdRegex3 = "http://t.sina.com.cn/(\\d+).*";
        Pattern pattern1 = Pattern.compile(userIdRegex1);
        Pattern pattern2 = Pattern.compile(userIdRegex2);
        Pattern pattern3 = Pattern.compile(userIdRegex3);
        HashMap<Pattern, Integer> retMap = new HashMap<>();
        retMap.put(pattern1, 2);
        retMap.put(pattern2, 2);
        retMap.put(pattern3, 1);
        return retMap;
    }

    static MongoCollection<Document> wbMediaColl = MongoUtils.instance().getDatabase(dbName).getCollection("wbMediaTest");

    public static void handleWbMedia(String homePageUrl, String tag, String retPath) {
        try {
            String userId = getStrByPatterns(homePageUrl, wbPatternMap);
            Document doc = null;
            if (StringUtils.isNotBlank(userId)) {
                doc = wbMediaColl.find(Filters.eq("_id", userId)).first();
                if (doc != null && !doc.isEmpty()) {
                    doc = ConvertUtils.convert2WbMediaDoc(doc, tag);
                    //媒体库不存在的情况
                } else {
                    UranusSdk.i().collect(userId, CollectType.C_WB_M_S, new JustOneTrigger(), Priority.L1);
                    doc = crawWbAccount(homePageUrl, tag);
                }
                //不能取到weiboId情况
            } else {
                doc = crawWbAccount(homePageUrl, tag);
            }
            if (doc != null) {
                doc = ConvertUtils.convert2WbMediaDoc(doc, tag);
                writeLine(JSON.toJSONString(doc), retPath);
                counter.addAndGet(1);
            } else {
                throw new RuntimeException("fail");
            }
        } catch (Exception e) {
            throw new RuntimeException("fail");
        }
    }

    private static Document crawWbAccount(String homePageUrl, String tag) {
        Optional<WeiboAccount> wbAccountOpt = wbSpider.getWeiboAccountByHomePage(homePageUrl);
        if (wbAccountOpt.isPresent()) {
            return ConvertUtils.convert2WbMediaDoc(wbAccountOpt.get(), tag);
        }
        return null;
    }


    static MongoCollection<Document> wxMediaColl = MongoUtils.instance().getDatabase(dbName).getCollection("wxMedia");

    public static void handleWxMedia(String wxId, String tag, String retPath) {
        try {
            Document doc = wxMediaColl.find(Filters.eq("weixinID", wxId)).first();
            if (doc == null || doc.isEmpty()) {
                Optional<List<GsData>> wxMediasOpt = GsDataWeixinParser.getWeixinAccountByIdOrName(wxId, 4);
                if (wxMediasOpt.isPresent()) {
                    List<GsData> collect = wxMediasOpt.get().stream().filter(wxMedia -> wxMedia.getWechatid().equalsIgnoreCase(wxId)).collect(Collectors.toList());
                    if (!collect.isEmpty()) {
                        GsData gsData = collect.get(0);
                        doc = ConvertUtils.convert2WxMediaDoc(gsData, tag);
                        UranusSdk.i().collect(wxId, CollectType.C_WX_M_S, new JustOneTrigger(), Priority.L1);
                    }
                }
            }
            if (doc != null && !doc.isEmpty()) {
                doc = ConvertUtils.convert2WxMediaDoc(doc, tag);
                writeLine(JSON.toJSONString(doc), retPath);
                counter.addAndGet(1);
            } else {
                throw new RuntimeException("fail");
            }
        } catch (Exception e) {
            throw new RuntimeException("fail");
        }
    }



    static AtomicLong counter = new AtomicLong(0);

    private static void writeLine(String line, String path) throws IOException {
        Path savePath = Paths.get(path);
        Files.write(savePath, (line + "\r\n").getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.SYNC);
    }

    public static void main(String[] args) throws IOException {
        start("G:/New-list.csv", "g:/ret.json");
    }
}
