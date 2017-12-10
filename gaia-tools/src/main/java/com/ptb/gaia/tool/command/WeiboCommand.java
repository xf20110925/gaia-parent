package com.ptb.gaia.tool.command;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.ptb.gaia.service.service.GWbArticleService;
import com.ptb.gaia.service.service.GWbMediaService;
import com.ptb.gaia.tool.utils.MysqlClient;
import com.ptb.uranus.spider.weibo.parse.WeiboTagParser;
import org.apache.commons.configuration.ConfigurationException;
import org.bson.Document;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by eric on 16/7/28.
 */
public class WeiboCommand {
    static GWbArticleService gWbMediaService;

    static {
        try {
            gWbMediaService = new GWbArticleService();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    static class WbMediaTag {
        private String pmid;
        private String tag;

        public String getPmid() {
            return pmid;
        }

        public void setPmid(String pmid) {
            this.pmid = pmid;
        }

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }
    }

    public static void ImportWbTagToMongo() {
        AtomicLong counter = new AtomicLong(0);
        try {
            GWbMediaService gWbMediaService = new GWbMediaService();
            MongoCollection<Document> wbMediaCollect = gWbMediaService.getWbMediaCollect();
            String sql = "select distinct pmid, tag from weibo_media_tag where tag != 'none'";
            List<WbMediaTag> wbMediaTags = MysqlClient.instance.query(sql, WbMediaTag.class);
            wbMediaTags.parallelStream().forEach(wbMediaTag -> {
                Document document = new Document("$set", new Document("tags", Arrays.asList(wbMediaTag.getTag())).append("updateTime", System.currentTimeMillis()));
                UpdateResult result = wbMediaCollect.updateOne(Filters.eq("_id", wbMediaTag.getPmid()), document, new UpdateOptions().upsert(false));
                System.out.printf("更新%s媒体成功，已更新%d个媒体\r\n", wbMediaTag.getPmid(), counter.addAndGet(result.getModifiedCount()));
            });
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

    }

    public static void fillMissingItagsMedia() {
    }

    private static int wbArticlePostTimeUpdate(int updateSize) throws ConfigurationException {
        FindIterable<Document> docs = gWbMediaService.getWbArticleCollectByPrimary().find(Filters.gte("postTime", System.currentTimeMillis() * 100)).limit(updateSize);
        LinkedList<UpdateOneModel<Document>> updateDocList = new LinkedList<>();
        for (Document document : docs) {
            updateDocList.add(new UpdateOneModel<>(Filters.eq("_id", document.getString("_id")), new Document("$set", new Document("postTime", document.getLong("postTime") / 1000))));
        }
        BulkWriteResult bulkWriteResult = gWbMediaService.getWbArticleCollectByPrimary().bulkWrite(updateDocList);
        return bulkWriteResult.getMatchedCount();
    }

    public static void update() {
        int counter = 0;
        int updateSize = 0;
        while (true) {
            try {
                updateSize = wbArticlePostTimeUpdate(1000);
            } catch (ConfigurationException e) {
                e.printStackTrace();
            }
            counter += updateSize;
            System.out.println(String.format("已更新%d条数据", counter));
            if (updateSize < 1000) {
                System.out.println(String.format("数据更新完毕共更新%d条数据", counter));
                return;
            }
        }
    }

    public static void weiboTagSpider() {
        FindIterable<Document> documents = gWbMediaService.getWbMediaCollectByPrimary().find(Filters.and(Filters.gt("isAuth", 0), Filters.gte("fansNum", 100000))).projection(new Document("_id", 1));
        WeiboTagParser parser = new WeiboTagParser();
        String sql = "insert into weibo_media_tag(pmid, tag) values(?,?)";
        List<String> pmids = new LinkedList<>();
        for (Document document : documents) {
            String pmid = document.getString("_id");
            pmids.add(pmid);

        }
        List<String> pmidsExists = MysqlClient.instance.queryForList("select pmid from weibo_media_tag", String.class);
        pmids.removeAll(pmidsExists);
        pmids.stream().forEach(pmid -> {
            try {
                String tag = parser.getTagByWeiboId(pmid);
                MysqlClient.instance.template.update(sql, pmid, tag);
            } catch (NoSuchElementException e) {
                MysqlClient.instance.template.update(sql, pmid, "none");
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
//        weiboTagSpider();
        ImportWbTagToMongo();
    }
}
