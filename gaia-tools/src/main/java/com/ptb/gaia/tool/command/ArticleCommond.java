package com.ptb.gaia.tool.command;

import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson.JSON;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.ptb.gaia.search.ISearch;
import com.ptb.gaia.search.ISearchImpl;
import com.ptb.gaia.service.IGaia;
import com.ptb.gaia.service.IGaiaImpl;
import com.ptb.gaia.service.entity.article.GWxArticle;
import com.ptb.gaia.service.service.GWbArticleService;
import com.ptb.gaia.service.service.GWxArticleService;
import com.ptb.gaia.tokenizer.GToken;
import com.ptb.gaia.tokenizer.JSegTextAnalyser;
import com.ptb.gaia.tokenizer.TextAnalyser;
import com.ptb.gaia.tokenizer.category.FastTestCategory;
import com.ptb.gaia.tool.esTool.util.MysqlDb;
import com.ptb.gaia.tool.utils.MongoUtils1;
import com.ptb.utils.exception.PTBException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.time.DateFormatUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Created by eric on 16/8/9.
 */
public class ArticleCommond {
    private static Logger logger = LoggerFactory.getLogger(ArticleCommond.class);
    static final UpdateOptions updateFalseOptions = new UpdateOptions().upsert(false);
    static final UpdateOptions updateTrueOptions = new UpdateOptions().upsert(true);
    static IGaia iGaia;
    static TextAnalyser textAnalyser;
    static ISearch iSearch;
    private static GWxArticleService gWxArticleService;
    private static GWbArticleService gWbArticleService;
    static Configuration conf;
    private ExecutorService executor;


    public ArticleCommond() {
        executor = Executors.newFixedThreadPool(8);
    }


    static {
        try {
            conf = new PropertiesConfiguration("ptb.properties");
            iGaia = new IGaiaImpl();
            textAnalyser = JSegTextAnalyser.i();
            iSearch = new ISearchImpl();
            gWxArticleService = new GWxArticleService();
            gWbArticleService = new GWbArticleService();
        } catch (Exception e) {
            throw new PTBException("GAIA配置文件不正常", e);
        }
    }

    // cat 1.txt  |./bin/tools.sh -ft -libPath /root/test/fastText/ljfasttext.so -modelPath ../result/dbpedia.bin > 2.txtvim 2.txt

    public static void category(String libpath, String modelPath) {
        FastTestCategory fastTestCategory = new FastTestCategory(libpath);
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String[] strings = fastTestCategory.predicatK(modelPath, scanner.nextLine(), 1);
            if (strings.length > 0) {
                System.out.println(strings[0]);
            }
        }
    }

    public static void dataPrepar() {
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String[] pmidCategory = scanner.nextLine().split(",");
            String pmid = pmidCategory[0];
            String categoryId = pmidCategory[1];
            if (StringUtils.isEmpty(pmid) || StringUtils.isEmpty(categoryId)) {
                continue;
            }

            List<GWxArticle> list = iSearch.contentSearchByPmid(pmid, GWxArticle.class, 0, 200);

            int total = list.size();

            System.err.println("pmid : " + pmid);

            if (list == null || total == 0) {
                continue;
            } else {
                int start = 0;
                int end = 0;
                for (int i = 0; i < total; i = i + 11) {
                    if ((i + 10) > total) {
                        start = i;
                        end = total;
                    } else {
                        start = i;
                        end = (i + 10);
                    }

                    list.stream().forEach(x -> {
                        List<GToken> gTokenList = textAnalyser.getTokens(x.getContent());
                        String word = org.apache.commons.lang.StringUtils.join(gTokenList.stream().map(GToken::getWord).collect(Collectors.toList()), " ");
                        System.out.println(String.format("__label__%s , %s", categoryId, word));
                    });
                }
            }

        }

    }


    private static void doTagForUesedMedia(String pmid, int type, String name) {
        Document append = new Document().append("$set", new Document("isUsed", 2));
        switch (type) {
            case 1:
                gWxArticleService.getWxMediaCollect().updateOne(Filters.eq("_id", pmid), append, updateFalseOptions);
                break;
            case 2:
                gWbArticleService.getWbMediaCollect().updateOne(Filters.eq("_id", pmid), append, updateFalseOptions);
                break;
        }
        System.out.println(String.format("%s doUseTagWith type [%d] ::: url [%s]", name, type, pmid));
    }


    private void delWxWbArticle(int type) {

        Calendar cal = Calendar.getInstance(Locale.CHINA);
        cal.add(Calendar.DATE, -90);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        long time = cal.getTimeInMillis();

        AtomicLong counter = new AtomicLong(0);
        FindIterable<Document> docs;
        LinkedList<String> docList = new LinkedList<>();
        if (type == 1) {
            docs = gWxArticleService.getWxArticleCollect().find().filter(Filters.lt("postTime", time)).noCursorTimeout(true);
        } else {
            docs = gWbArticleService.getWbArticleCollect().find().filter(Filters.lt("postTime", time)).noCursorTimeout(true);
        }

        for (Document doc : docs) {
            if (doc.getInteger("isUsed") != null && doc.getInteger("isUsed") == 10) {
                continue;
            }

//            if(type==2){
//                if (doc.getLong("postTime") == null || doc.getLong("postTime") >= time) {
//                    continue;
//                }
//            }

            counter.addAndGet(1);
            docList.add(doc.getString("_id"));

            if (docList.size() >= 5000) {
                System.out.println("够5000了!!!!");
                LinkedList<String> threadList = new LinkedList<>(docList);
                if (type == 1) {
                    executor.execute(() -> {
                        System.out.println(" 处理开始: " + Thread.currentThread().getName() + "!!!!!!!");
                        gWxArticleService.getWxArticleCollectByPrimary().deleteMany(new Document("_id", new Document("$in", threadList)));
                        System.out.println(" 处理结束: " + Thread.currentThread().getName() + "!!!!!!!");
                    });
                } else {
                    executor.execute(() -> {
                        System.out.println(" 处理开始: " + Thread.currentThread().getName() + "!!!!!!!");
                        gWbArticleService.getWbArticleCollectByPrimary().deleteMany(new Document("_id", new Document("$in", threadList)));
                        System.out.println(" 处理结束: " + Thread.currentThread().getName() + "!!!!!!!");

                    });
                }
                docList.clear();
                System.out.println(String.format("已导出%s个文章", counter));
            }
        }
        //收尾
        LinkedList<String> lastList = new LinkedList<>(docList);
        if (CollectionUtils.isNotEmpty(lastList)) {
            if (type == 1) {
                executor.execute(() -> gWxArticleService.getWxArticleCollectByPrimary().deleteMany(new Document("_id", new Document("$in", lastList))));
            } else {
                executor.execute(() -> gWbArticleService.getWbArticleCollectByPrimary().deleteMany(new Document("_id", new Document("$in", lastList))));
            }
        }
        executor.shutdown();
        ExecutorStatus(executor);
    }


    private void ExecutorStatus(ExecutorService executor) {
        while (true) {
            if (executor.isTerminated()) {
                System.out.println("所有任务执行完毕");
                break;
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private static void doTagForUesedArticle(String url, int type, String name) {
        Document append = new Document().append("$set", new Document("isUsed", 10));
        switch (type) {
            case 1:
                gWxArticleService.getWxArticleCollectByPrimary().updateOne(Filters.eq("_id", url), append, updateFalseOptions);
                break;
            case 2:
                gWbArticleService.getWbArticleCollectByPrimary().updateOne(Filters.eq("_id", url), append, updateFalseOptions);
                break;
        }
        System.out.println(String.format("%s doUseTagWith type [%d] ::: url [%s]", name, type, url));
    }


    private static void doTagForUesedArticle(String articleUrl, int type) {
        Document append = new Document().append("$set", new Document("isUsed", 2));
        switch (type) {
            case 1:
                gWxArticleService.getWxArticleCollect().updateOne(Filters.eq("_id", articleUrl), append);
                break;
            case 2:
                gWbArticleService.getWbArticleCollect().updateOne(Filters.and(Filters.eq("_id", articleUrl), Filters.ne("isUsed", 1)), append);
                break;
        }
        System.out.println(String.format("doUseTagWith type [%d] ::: url [%s]", type, articleUrl));
    }

    private static void doUsedTagForZeusMysqlArticle() throws SQLException {
        //读取used article
        String sql = "SELECT DISTINCT\n" + "  url,\n" + "  plat_type\n" + "FROM (\n" + "       SELECT\n" + "         a.url,\n" + "         b.plat_type\n" + "       FROM article_channel_detail a\n" + "         JOIN article_channel b ON a.acid = b.id\n" + "       UNION ALL\n" + "       SELECT\n" + "         url,\n" + "         1 AS ptype\n" + "       FROM hot_article\n" + "       UNION ALL\n" + "       SELECT\n" + "         article_id,\n" + "         `type`\n" + "       FROM user_article_mapping\n" + "       UNION ALL\n" + "       SELECT\n" + "         article_url,\n" + "         2 AS ptype\n" + "       FROM wb_topic_article\n" + "       UNION ALL\n" + "       SELECT\n" + "         url,\n" + "         `type`\n" + "       FROM user_article\n" + "       UNION ALL\n" + "       SELECT\n" + "         url,\n" + "         `ptype`\n" + "       FROM headlines) t1;";
        MysqlDb mysqlDb = new MysqlDb(sql);
        ResultSet rs = mysqlDb.pst.executeQuery();
        try {
            rs = mysqlDb.pst.executeQuery();
            while (rs.next()) {
                String articleUrl = rs.getString(1);
                int type = rs.getInt(2);
                doTagForUesedArticle(articleUrl, type);
            }
            rs.close();
            mysqlDb.conn.close();
            mysqlDb.pst.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void doUsedTagForZeusMysqlMedia(int type) throws SQLException {
        String sql = "";
        if (type == 2) {
            sql = "select DISTINCT p.pmid from (select a.pmid from headlines a where a.ptype=2 UNION all select a.pmid from media_channel_detail a where a.pmid not like'M%' union all select b.p_mid as pmid from user_media b where b.type=2 union all select c.pmid from user_article c where c.type=2 union all select d.pmid from media_category d where d.type=2) p;";
        } else {
            sql = "SELECT DISTINCT p.pmid FROM (SELECT a.pmid FROM headlines a WHERE a.ptype=1 UNION ALL SELECT a.pmid FROM media_channel_detail a WHERE a.pmid LIKE'M%' UNION ALL SELECT b.p_mid AS pmid FROM user_media b WHERE b.type=1 UNION ALL SELECT c.pmid FROM user_article c WHERE c.type=1 UNION ALL SELECT d.pmid FROM media_category d WHERE d.type=1) p;";
        }
        MysqlDb mysqlDb = new MysqlDb(sql);
        ResultSet rs = mysqlDb.pst.executeQuery();
        try {
            rs = mysqlDb.pst.executeQuery();
            while (rs.next()) {
                String pmid = rs.getString(1);
                doTagForUesedMedia(pmid, type, "From Mysql : ");
            }
            rs.close();
            mysqlDb.conn.close();
            mysqlDb.pst.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void doUsedTagForZeusMysqlArticle(int type) throws SQLException {
        String sql;
        if (type == 1) {
            sql = "select url from zeus.user_article where type=1 union all select url from zeus.article_channel t join zeus.article_channel_detail t1 on t.id=t1.acid where t.plat_type=1";
        } else {
            sql = "select url from zeus.user_article where type=2 union all select url from zeus.article_channel t join zeus.article_channel_detail t1 on t.id=t1.acid where t.plat_type=2";
        }
        MysqlDb mysqlDb = new MysqlDb(sql);
        ResultSet rs = mysqlDb.pst.executeQuery();
        String name;
        if (type == 1) {
            name = "From Mysql : WeinXinArticle";
        } else {
            name = "From Mysql : WeinBoArticle";
        }
        try {
            rs = mysqlDb.pst.executeQuery();
            while (rs.next()) {
                String url = rs.getString(1);
                doTagForUesedArticle(url, type, name);
            }
            rs.close();
            mysqlDb.conn.close();
            mysqlDb.pst.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    static class row {
        int platType;
        String id;

        public int getPlatType() {
            return platType;
        }

        public void setPlatType(int platType) {
            this.platType = platType;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }

    static class rowMedia {

        String pmid;

        public String getPmid() {
            return pmid;
        }

        public void setPmid(String pmid) {
            this.pmid = pmid;
        }

    }

    static class rowId {

        String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }


    private static void doUsedTagForZeusMongoArticle() {
        MongoDatabase zeus = gWxArticleService.mongoClient.getDatabase("zeus");
        MongoCollection<Document> article = zeus.getCollection("article");
        FindIterable<Document> projection = article.find().projection(new Document().append("_id", 1).append("platType", 1));
        for (Document document : projection) {
            Object remove = document.append("id", document.get("_id")).remove("_id");
            row row = JSON.parseObject(JSON.toJSONString(document), row.class);
            doTagForUesedArticle(row.getId(), row.getPlatType());
        }


    }

    private static void doUsedTagNoUpdateTimeMedia(int type) {

        Document append = new Document().append("$set", new Document("isUsed", 2));

        MongoDatabase zeus = gWxArticleService.mongoClient.getDatabase("gaia2");
        MongoCollection<Document> media;
        if (type == 2) {
            media = zeus.getCollection("wbMedia");
        } else {
            media = zeus.getCollection("wxMedia");
        }
        List<String> pmidList = new ArrayList<>();

        FindIterable<Document> projection = media.find().filter(Filters.and(Filters.exists("updateTime", false), Filters.ne("isUsed", 2))).projection(new Document().append("pmid", 1)).noCursorTimeout(true);
        for (Document document : projection) {
            String pmid = document.getString("pmid");
            if (!StringUtils.isEmpty(pmid) && pmid != "") {
                System.out.println("From No UpdateTime : " + pmid);
                pmidList.add(pmid);
            }
            if (pmidList.size() > 1000) {
                System.out.println("Remove Many Date : " + pmidList.size());
                media.updateMany(new Document("_id", new Document("$in", pmidList)), append, updateFalseOptions);
                pmidList.clear();
            }
        }
        if (pmidList.size() > 0) {
            System.out.println("Remove Many Date : " + pmidList.size());
            media.updateMany(new Document("_id", new Document("$in", pmidList)), append, updateFalseOptions);
            pmidList.clear();
        }

    }


    //打标记 微博fansNum大于10000  设置 isUsed=1
    private static void doUsedTagFansNumMedia(int type) {
        Document append = new Document().append("$set", new Document("isUsed", 2));
        switch (type) {
            case 1:
//                gWxArticleService.getWxMediaCollect().deleteMany(Filters.and(Filters.lt("updateTime", timeLimit), Filters.ne("isUsed", 1)));
                break;
            case 2:
                System.out.println("Start Update FansNum :");
                long t1 = System.currentTimeMillis(); //排序前取得当前时间
                List<String> pmidList = new ArrayList<>();

                FindIterable<Document> projection = gWbArticleService.getWbMediaCollect().find().projection(new Document().append("_id", 1).append("fansNum", 1)).noCursorTimeout(true);
                int fansNum = 0;

                for (Document document : projection) {

                    if (document.getInteger("fansNum") != null) {
                        fansNum = document.getInteger("fansNum");
                    } else {
                        continue;
                    }

                    if (fansNum >= 10000) {
                        String pmid = document.getString("_id");
                        if (!StringUtils.isEmpty(pmid)) {
                            System.out.println("From FansNum UpdateTime : " + pmid);
                            pmidList.add(pmid);
                        }
                        if (pmidList.size() > 1000) {
                            System.out.println("Remove FansNum Many Date : " + pmidList.size());
                            gWbArticleService.getWbMediaCollect().updateMany(new Document("_id", new Document("$in", pmidList)), append, updateFalseOptions);
                            pmidList.clear();
                        }
                    }

                }
                if (pmidList.size() > 0) {
                    System.out.println("Remove FansNum Many Date : " + pmidList.size());
                    gWbArticleService.getWbMediaCollect().updateMany(new Document("_id", new Document("$in", pmidList)), append, updateFalseOptions);
                    pmidList.clear();
                }

                long t2 = System.currentTimeMillis(); //排序后取得当前时间
                Calendar c = Calendar.getInstance();
                c.setTimeInMillis(t2 - t1);
                System.out.println(" 耗时: " + c.get(Calendar.MINUTE) + "分 " + c.get(Calendar.SECOND) + "秒 " + c.get(Calendar.MILLISECOND) + " 微秒");
                System.out.println("End Update FansNum");

                break;
        }
    }

    //打标签 30天内发文的媒体打标签
    private static void doUsedTagWxMedia30(int type, long timeLimit) {
        Document append = new Document().append("$set", new Document("isUsed", 2));
        switch (type) {
            case 1:
                System.out.println("Start Update WX 30 :");
                long t1 = System.currentTimeMillis(); //排序前取得当前时间
                List<String> pmidList = new ArrayList<>();

                FindIterable<Document> projection = gWxArticleService.getWxMediaCollect().find().projection(new Document().append("_id", 1).append("latestArticle", 1).append("wxStatisticsIn30Day", 1)).noCursorTimeout(true);
                Long time = 0L;
                String avgHeadReadNumInPeroid = "0";

                for (Document document : projection) {

                    if (document.get("latestArticle") == null && document.get("wxStatisticsIn30Day") == null) {
                        continue;
                    }

                    if (document.get("latestArticle") != null) {
                        Document latestArticle = (Document) document.get("latestArticle");
                        time = Long.valueOf(latestArticle.getString("time"));
                    }

                    if (document.get("wxStatisticsIn30Day") != null) {
                        Document wxStatisticsIn30Day = (Document) document.get("wxStatisticsIn30Day");
                        try {
                            avgHeadReadNumInPeroid = String.valueOf(wxStatisticsIn30Day.getInteger("avgHeadReadNumInPeroid"));
                        } catch (Exception e) {
                            avgHeadReadNumInPeroid = String.valueOf(wxStatisticsIn30Day.getLong("avgHeadReadNumInPeroid"));
                        }
                    }

                    if (time >= timeLimit || Long.valueOf(avgHeadReadNumInPeroid) >= 200L) {
                        String pmid = document.getString("_id");
                        if (!StringUtils.isEmpty(pmid)) {
                            System.out.println("From wx30 UpdateTime : " + pmid);
                            pmidList.add(pmid);
                        }
                        if (pmidList.size() > 1000) {
                            System.out.println("Remove wx30 Many Date : " + pmidList.size());
                            gWxArticleService.getWxMediaCollect().updateMany(new Document("_id", new Document("$in", pmidList)), append, updateFalseOptions);
                            pmidList.clear();
                        }
                    }

                }
                if (pmidList.size() > 0) {
                    System.out.println("Remove wx30 Many Date : " + pmidList.size());
                    gWxArticleService.getWxMediaCollect().updateMany(new Document("_id", new Document("$in", pmidList)), append, updateFalseOptions);
                    pmidList.clear();
                }

                long t2 = System.currentTimeMillis(); //排序后取得当前时间
                Calendar c = Calendar.getInstance();
                c.setTimeInMillis(t2 - t1);
                System.out.println(" 耗时: " + c.get(Calendar.MINUTE) + "分 " + c.get(Calendar.SECOND) + "秒 " + c.get(Calendar.MILLISECOND) + " 微秒");
                System.out.println("End Update wx30");
                break;
            case 2:
                break;
        }
    }


    private static void doUsedTagForZeusMongoMedia(int type) {
        //mongo  zeus：article、share、gshare
        List<Document> list;
        List<String> shareList;

        MongoDatabase zeus = gWxArticleService.mongoClient.getDatabase("zeus");
        MongoCollection<Document> article = zeus.getCollection("article");
        FindIterable<Document> projection = article.find().filter(Filters.eq("platType", type)).projection(new Document().append("pmid", 1));
        for (Document document : projection) {
            rowMedia row = JSON.parseObject(JSON.toJSONString(document), rowMedia.class);
            if (!StringUtils.isEmpty(row.getPmid()) && row.getPmid() != "") {
                doTagForUesedMedia(row.getPmid(), type, "From MongoDb:zeus.article");
            }
        }

        MongoCollection<Document> shareArticle = zeus.getCollection("share");
        FindIterable<Document> shareProjection = shareArticle.find().filter(Filters.eq("type", type)).projection(new Document().append("pmids", 1));
        for (Document document : shareProjection) {
            shareList = (ArrayList<String>) document.get("pmids");
            if (CollectionUtils.isNotEmpty(shareList)) {
                shareList.stream().forEach(x -> {
                    if (!StringUtils.isEmpty(x) && x != "") {
                        doTagForUesedMedia(x, type, "From MongoDb pmids:zeus.share");
                    }
                });
                shareList.clear();
            }
        }

        FindIterable<Document> shareProjection1 = shareArticle.find().filter(Filters.eq("type", type)).projection(new Document().append("pMids", 1));
        for (Document document : shareProjection1) {
            shareList = (ArrayList<String>) document.get("pMids");
            if (CollectionUtils.isNotEmpty(shareList)) {
                shareList.stream().forEach(x -> {
                    if (!StringUtils.isEmpty(x) && x != "") {
                        doTagForUesedMedia(x, type, "From MongoDb pMids:zeus.share");
                    }
                });
                shareList.clear();
            }
        }

        MongoCollection<Document> gshare = zeus.getCollection("gshare");
        FindIterable<Document> gshare_projection = gshare.find().projection(new Document().append("groupList.map", 1));
        for (Document document : gshare_projection) {
            list = (ArrayList<Document>) document.get("groupList");
            if (CollectionUtils.isNotEmpty(list)) {
                list.stream().forEach(x -> {
                    List<Document> listMap = (ArrayList<Document>) x.get("map");
                    if (CollectionUtils.isNotEmpty(listMap)) {
                        listMap.stream().filter(y -> (Integer) y.get("type") == type).forEach(y -> {
                            if (!StringUtils.isEmpty((String) y.get("pmid")) && (String) y.get("pmid") != "") {
                                doTagForUesedMedia((String) y.get("pmid"), type, "From MongoDb:zeus.gshare");
                            }
                        });
                    }
                });
                list.clear();
            }
        }

    }


    private static void doUsedTag() throws SQLException, ConfigurationException {
        doUsedTagForZeusMysqlArticle();
        doUsedTagForZeusMongoArticle();
    }

    public static void doUsedMediaTag(int type, long timeLimit) throws SQLException, ConfigurationException {

        Calendar c = Calendar.getInstance();
        System.out.println("Start Update Mysql And Mongo");
        long startTime = System.currentTimeMillis(); //排序前取得当前时间
        doUsedTagForZeusMysqlMedia(type);  //mysql 数据标记
        doUsedTagForZeusMongoMedia(type); //mongodb 数据标记
        long EndTime = System.currentTimeMillis(); //排序后取得当前时间
        c.setTimeInMillis(EndTime - startTime);
        System.out.println(" 耗时: " + c.get(Calendar.MINUTE) + "分 " + c.get(Calendar.SECOND) + "秒 " + c.get(Calendar.MILLISECOND) + " 微秒");
        System.out.println("End Mysql And Mongo");

        if (type == 2) {
            doUsedTagFansNumMedia(type); //粉丝数大于10000并且 isUsed不等于1的
        } else {
            //更新30天内发文的媒体标签 or 30天平均阅读数大于200的打标记
            doUsedTagWxMedia30(type, timeLimit);
        }


        //更新 没有 updatetime的文章  表示 isUsed 为1 说明这些可能是新添加的媒体 还没有爬取
//        doUsedTagNoUpdateTimeMedia(type);
    }

    private static void delMedia(long timeLimit, int type) {
        switch (type) {
            case 1:
//                gWxArticleService.getWxMediaCollect().deleteMany(Filters.and(Filters.lt("updateTime", timeLimit), Filters.ne("isUsed", 1)));
                break;
            case 2:
                FindIterable<Document> findIterable = gWxArticleService.getWbMediaCollect().find().projection(new Document().append("_id", 1).append("updateTime", 1).append("isUsed", 1)).noCursorTimeout(true);
                List<String> list2 = new ArrayList<>();
                List<String> list3 = new ArrayList<>();
                for (Document document : findIterable) {

                    if (document.getInteger("isUsed") != null) {
                        continue;
                    }

                    if (document.getLong("updateTime") == null || document.getLong("updateTime") > timeLimit) {
                        continue;
                    }

                    String pmid = (String) document.get("_id");
                    list3.add(pmid);
                    try {
                        if (!StringUtils.isEmpty(pmid) && pmid != "") {
                            System.out.println("Start Delete pmid : " + pmid);
                            //删除pmid对应的文章信息
                            FindIterable<Document> findIterable1 = gWxArticleService.getWbArticleCollect().find(Filters.eq("pmid", pmid)).projection(new Document().append("_id", 1)).noCursorTimeout(true);
                            for (Document document1 : findIterable1) {
                                list2.add(document1.getString("_id"));
                            }
                        }
                        if (list2.size() > 1000) {
                            System.out.println("remove Article many : " + list2.size());
                            gWxArticleService.getWbArticleCollect().deleteMany(new Document("_id", new Document("$in", list2)));
                            list2.clear();
                        }
                        //删除pmid对应的媒体信息
                        if (list3.size() > 1000) {
                            System.out.println("remove Media many : " + list3.size());
                            gWxArticleService.getWbMediaCollect().deleteMany(new Document("_id", new Document("$in", list3)));
                            list3.clear();
                        }
                    } catch (Exception e) {
                        System.out.println(e.toString());
                        System.out.println("Error pmid : " + pmid);
                    }
                }
                if (list2.size() > 0) {
                    System.out.println("remove Article many : " + list2.size());
                    gWxArticleService.getWbArticleCollect().deleteMany(new Document("_id", new Document("$in", list2)));
                    list2.clear();
                }
                if (list3.size() > 0) {
                    System.out.println("remove Media many : " + list3.size());
                    gWxArticleService.getWbMediaCollect().deleteMany(new Document("_id", new Document("$in", list3)));
                    list3.clear();
                }
                break;
        }
    }

    private static void delArticle(long timeLimit) {
        gWxArticleService.getWxArticleCollect().deleteMany(Filters.and(Filters.or(Filters.lt("postTime", timeLimit), Filters.lt("updateTime", timeLimit)), Filters.ne("isUsed", 1)));
        //gWbArticleService.getWbArticleCollect().deleteMany(Filters.and(Filters.or(Filters.lt("postTime", timeLimit), Filters.lt("addTime", timeLimit)), Filters.ne("isUsed", 1)));
    }

    public static void agedArticle(long timeLimit) throws SQLException, ConfigurationException {
        System.out.println(String.format("10秒后开始删除 %s之前的文章数据", DateFormatUtils.format(new Date(timeLimit),
                "yyyy-MM-dd hh:mm:ss"), Locale.CHINA));
        //flag the used article
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {

        }
        System.out.println("正在进行打使用标签操作：【有使用标签的数据将不会受删除影响......】");
        doUsedTag();
        System.out.println("删除过期数据中......请稍后");
        delArticle(timeLimit);
    }

    public static void agedMedia(long timeLimit, int type) throws SQLException, ConfigurationException {
        System.out.println(String.format("10秒后开始打标记 %s之前的媒体数据 媒体数据类型为: " + type, DateFormatUtils.format(new Date(timeLimit),
                "yyyy-MM-dd hh:mm:ss"), Locale.CHINA));

        System.out.println("正在进行打使用标签操作");
        doUsedMediaTag(type, timeLimit);
        System.out.println("使用标签操作结束完成");

      /*  System.out.println("正式删除数据 : ");
        System.out.println("删除过期数据中........");
        long delStartTime = System.currentTimeMillis(); //排序前取得当前时间
        delMedia(timeLimit, type);
        long delEndTime = System.currentTimeMillis(); //排序后取得当前时间
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(delEndTime - delStartTime);
        System.out.println(" 耗时: " + c.get(Calendar.MINUTE) + "分 " + c.get(Calendar.SECOND) + "秒 " + c.get(Calendar.MILLISECOND) + " 微秒");
        System.out.println("删除过期数据结束,请检查");*/
    }


    public static void agedWxWbArticle(int type) throws SQLException, ConfigurationException {

        System.out.println("正在进行打使用标签操作");
        doUsedTagForZeusMysqlArticle(type);
        System.out.println("标签操作结束完成");

        System.out.println("正式删除数据 : ");
        System.out.println("删除过期数据中........");
        long delStartTime = System.currentTimeMillis(); //排序前取得当前时间
        new ArticleCommond().delWxWbArticle(type);
        long delEndTime = System.currentTimeMillis(); //排序后取得当前时间
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(delEndTime - delStartTime);
        System.out.println(" 耗时: " + c.get(Calendar.MINUTE) + "分 " + c.get(Calendar.SECOND) + "秒 " + c.get(Calendar.MILLISECOND) + " 微秒");
        System.out.println("删除过期数据结束,请检查");
    }


    public static void PillDataToMongodb() throws SQLException, ConfigurationException {

        String sql = "select url from zeus.article_channel_detail";

        MysqlDb mysqlDb = new MysqlDb(sql);
        ResultSet rs = mysqlDb.pst.executeQuery();
        List<String> list = new ArrayList<String>();
        try {
            rs = mysqlDb.pst.executeQuery();
            while (rs.next()) {
                String url = rs.getString(1);
                list.add(url);
            }
            rs.close();
            mysqlDb.conn.close();
            mysqlDb.pst.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("一共需要导入mongodb数据量 : " + list.size());

        AtomicLong counter = new AtomicLong(0);

        FindIterable<Document> docs = MongoUtils1.instance().getDatabase("gaia2").getCollection("wxArticle")
                .find(new Document("_id", new Document("$in", list)));

        for (Document doc : docs) {
            Document append = new Document().append("$set", doc);
            counter.addAndGet(1);
            String url = doc.getString("_id");
            gWxArticleService.getWxArticleCollectByPrimary().updateOne(Filters.eq("_id", url), append, updateTrueOptions);
            System.out.println("已插入数据 : " + url);
        }

        System.out.println("一共插入数据 : " + counter.get());

    }


    public static void main(String[] args) throws SQLException, ConfigurationException {
//        doUsedMediaTag(2);

     /*   FindIterable<Document> findIterable = gWxArticleService.getWbMediaCollect().find(Filters.exists("isUsed", false));
        int count = 0;
        for (Document document : findIterable) {
            count++;
        }

        System.out.println(count);*/

//        FindIterable<Document> findIterable= gWxArticleService.getWbMediaCollect().find(Filters.ne("isUsed", 1)).projection(new Document().append("pmid", 1));
//        for (Document document : findIterable) {
//            rowMedia row = JSON.parseObject(JSON.toJSONString(document), rowMedia.class);
//            if(!StringUtils.isEmpty(row.getPmid()) && row.getPmid()!=""){
//                doTagForUesedMedia(row.getPmid(), 2, "From MongoDb:zeus.article");
//            }
//        }


//        MongoDatabase zeus = gWxArticleService.mongoClient.getDatabase("test");
//        MongoCollection<Document> article = zeus.getCollection("wbMedia");
//        FindIterable<Document> projection = article.find(Filters.and(Filters.gte("fansNum", 10000), Filters.ne("isUsed", 1)));
//        for (Document document : projection) {
//            rowMedia row = JSON.parseObject(JSON.toJSONString(document), rowMedia.class);
//            if(!StringUtils.isEmpty(row.getPmid()) && row.getPmid()!=""){
//                doTagForUesedMedia(row.getPmid(), 2, "From MongoDb:zeus.article");
//            }
//        }


    }

}
