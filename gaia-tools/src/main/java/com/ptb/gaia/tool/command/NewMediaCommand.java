package com.ptb.gaia.tool.command;

import com.mongodb.client.FindIterable;
import com.ptb.gaia.service.entity.media.GWxMedia;
import com.ptb.gaia.service.service.GWbMediaService;
import com.ptb.gaia.service.service.GWxMediaService;
import com.ptb.gaia.service.utils.ConvertUtils;
import com.ptb.uranus.common.entity.CollectType;
import com.ptb.uranus.schedule.model.Priority;
import com.ptb.uranus.schedule.trigger.JustOneTrigger;
import com.ptb.uranus.schedule.trigger.PeriodicTrigger;
import com.ptb.uranus.sdk.UranusSdk;
import com.ptb.uranus.spider.weibo.bean.WeiboAccount;
import com.ptb.uranus.spider.weixin.WeixinSpider;
import com.ptb.uranus.spider.weixin.bean.GsData;
import com.ptb.uranus.spider.weixin.bean.WxAccount;
import com.ptb.utils.string.RegexUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ptb.gaia.tool.command.MediaCommand.weiboSpider;

/**
 * @DESC:
 * @VERSION:
 * @author: xuefeng
 * @Date: 2016/11/23
 * @Time: 16:47
 */
public class NewMediaCommand {
    /**
     * @param inputPath  文件读取路径
     * @param outputPath 丢失媒体导出路径
     * @param dbName     mongo 库名称
     * @param collecName 集合名称
     */
    public static void getMissingPmids(String inputPath, String outputPath, String dbName, String collecName) throws IOException {
        Set<String> pmids = getPmids(dbName, collecName);
        System.out.println(pmids.size());
        List<String> missingPmids = Files.lines(Paths.get(inputPath), Charset.forName("GBK")).parallel().filter(pmid -> !pmids.contains(pmid)).collect(Collectors.toList());
        Files.write(Paths.get(outputPath), missingPmids, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static Set<String> getPmids(String dbName, String collecName) {
        FindIterable<Document> docs = WxArticleReload.MongoUtil.INSTACENEWMONGO.getCollection(dbName, collecName).find().projection(new Document().append("_id", 1));
        Set<String> pmids = new HashSet<>();
        for (Document doc : docs) {
            pmids.add(doc.getString("_id"));
        }
        return pmids;
    }

    public static void missingWXMedia2Mongo(String inputPath) throws ConfigurationException, IOException {
        WeixinSpider weixinSpider = new WeixinSpider();
        List<String> notFoundPmids = new LinkedList<>();
        List<GWxMedia> wxMedias = Files.lines(Paths.get(inputPath)).map(wxId -> {
            Optional<List<GsData>> datasOpt = weixinSpider.getWeixinAccountByIdOrName(wxId, 1);
            if (datasOpt.isPresent()) {
                List<GsData> medias = datasOpt.get();
                if (medias == null || medias.isEmpty()) {
                    notFoundPmids.add(wxId);
                    return null;
                }
                WxAccount wxAccount = new WxAccount();
                GsData media = medias.get(0);
                String url = media.getIncluded();
                String biz = RegexUtils.sub(".*__biz=([^#&]*).*", url, 0);
                wxAccount.setBiz(biz);
                wxAccount.setId(media.getWechatid());
                wxAccount.setBrief(media.getFunctionintroduce());
                String headImage = String.format("http://open.weixin.qq.com/qr/code/?username=%s", media.getWechatid());
                wxAccount.setHeadImg(headImage);
                wxAccount.setNickName(media.getWechatname());
                wxAccount.setQcCode(headImage);
                return ConvertUtils.convert2GWxMedia(wxAccount);
            }
            notFoundPmids.add(wxId);
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());
        System.out.println(String.format("fail media -> %s", notFoundPmids));
        GWxMediaService wxService = new GWxMediaService();
        wxService.insertBatch(wxMedias);
        Files.write(Paths.get(inputPath), notFoundPmids, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    public static void missingWBMedia2Mongo(String inputPath) throws ConfigurationException, IOException {
        Files.lines(Paths.get(inputPath)).forEach(wbId -> {
            Optional<WeiboAccount> weiboAccountOpt = weiboSpider.getWeiboAccountByWeiboID(wbId);
            weiboAccountOpt.ifPresent(weiboAccount -> {
                GWbMediaService gWxMediaService = null;
                try {
                    gWxMediaService = new GWbMediaService();
                } catch (ConfigurationException e) {
                    e.printStackTrace();
                }
                gWxMediaService.insertBatch(Arrays.asList(ConvertUtils.convert2GWbMedia(weiboAccount)));
                gWxMediaService.insertBatch(Arrays.asList(ConvertUtils.ConverDynamic(weiboAccount)));
            });
        });
    }

    public static void uranusScheduleAddMedias(int platType, String pmid) {
        switch (platType) {
            case 1:
                UranusSdk.i().collect(pmid, CollectType.C_WX_M_S, new JustOneTrigger(new Date().getTime()), Priority.L1);
                break;
            case 2:
                UranusSdk.i().collect(pmid, CollectType.C_WB_M_S, new JustOneTrigger(new Date().getTime()), Priority.L1);
                UranusSdk.i().collect(pmid, CollectType.C_WB_M_D, new JustOneTrigger(new Date().getTime()), Priority.L1);
                break;
        }

    }

    public static void addSchedule(String filePath, String collectType) throws IOException {
        CollectType collectTypeEnum = CollectType.valueOf(collectType);
        List<String> pmids = Files.lines(Paths.get(filePath)).filter(StringUtils::isNotBlank).map(pmid -> String.format("%s:::%s", pmid, System.currentTimeMillis() / 1000 - 7 * 24 * 3600)).collect(Collectors.toList());
        System.out.println("开始插入调度。。。");
        UranusSdk.i().collect(pmids, collectTypeEnum, new PeriodicTrigger(720, TimeUnit.MINUTES, new Date(),
                new Date(Long.MAX_VALUE)), Priority.L1);
    }

    public static void main(String[] args) throws IOException, ConfigurationException {
        addSchedule("e:/weibopmids.txt", "C_WB_L_A_N");
    }

}
