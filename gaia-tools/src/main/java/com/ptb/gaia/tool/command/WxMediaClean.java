package com.ptb.gaia.tool.command;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.ptb.gaia.etl.flume.utils.FTPClientConfigure;
import com.ptb.gaia.etl.flume.utils.Tools;
import com.ptb.gaia.service.IGaia;
import com.ptb.gaia.service.IGaiaImpl;
import com.ptb.gaia.service.entity.article.GWxArticleBasic;
import com.ptb.gaia.service.service.GWbArticleService;
import com.ptb.gaia.service.service.GWbMediaService;
import com.ptb.gaia.service.service.GWxArticleService;
import com.ptb.gaia.tool.utils.JedisUtil;
import com.ptb.gaia.tool.utils.MongoUtils1;
import com.ptb.utils.date.DateUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.jsoup.Jsoup;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static com.ptb.gaia.etl.flume.utils.FileUtils.uploadFileToFTPWithDirBatch;
import static com.ptb.gaia.etl.flume.utils.FileUtils.uploadFileToFTPWithDirBatchNew;
import static com.ptb.gaia.etl.flume.utils.Tools.md5Password;

/**
 * @DESC:
 * @VERSION:
 * @author: xuefeng
 * @Date: 2016/11/3
 * @Time: 13:29
 */
public class WxMediaClean {
    public static final int fansNum = 10000;
    public static final String newMCollecName = "wxMedia";
    public static final String newACollecName = "wxArticle";
    static final UpdateOptions updateFalseOptions = new UpdateOptions().upsert(false);
    private GWbMediaService wbMservice;
    private GWbArticleService wbAservice;
    private GWxArticleService wxAservice;
    private ExecutorService executor;
    private IGaia iGaia;
    static String wxFtpPath = "";
    static FTPClientConfigure ftpClientConfigure;


    public WxMediaClean() throws ConfigurationException {
        executor = Executors.newFixedThreadPool(4);
        wbMservice = new GWbMediaService();
        wbAservice = new GWbArticleService();
        wxAservice = new GWxArticleService();
        iGaia = new IGaiaImpl();
        ftpClientConfigure = new FTPClientConfigure();
        wxFtpPath = ftpClientConfigure.getRelativedir();
    }

    private void insertMediaToRedis(LinkedList<String> docList) {
        LinkedList<String> pmids = new LinkedList<>(docList);
        executor.execute(() -> {
            for (String pmid : pmids) {

                JedisUtil.set(pmid.getBytes(), "1".getBytes());
                counter.getAndAdd(1);

            }
            System.out.println(String.format("已导出%s媒体", counter));
        });
    }


    public void wxMediaToRedis() throws ConfigurationException {
        AtomicLong counter = new AtomicLong(0);
        LinkedList<String> docList = new LinkedList<>();

        FindIterable<Document> wxDocs = wxAservice.getWxMediaCollect().find().projection(new Document().append("_id", 1));

        for (Document doc : wxDocs) {
            counter.addAndGet(1);
            docList.add(doc.getString("_id"));
            System.out.println("总数 : " + counter);
        }

        System.out.println("总数 : " + counter);

        AtomicLong counter1 = new AtomicLong(0);

        docList.parallelStream().forEach(pmid -> {
            counter1.addAndGet(1);
            if(StringUtils.isNotEmpty(pmid)){
                JedisUtil.set(pmid.getBytes(), "1".getBytes());
            }
            System.out.println("计数 ： " + counter1);
        });

    }


    public void wbMediaToRedis() throws ConfigurationException {
        AtomicLong counter = new AtomicLong(0);
        LinkedList<String> docList = new LinkedList<>();

        FindIterable<Document> wbDocs = wbMservice.getWbMediaCollect().find().projection(new Document().append("_id", 1));


        for (Document doc : wbDocs) {
            counter.addAndGet(1);
            docList.add(doc.getString("_id"));
        }

        System.out.println("总数 : " + counter);

        AtomicLong counter1 = new AtomicLong(0);

        docList.parallelStream().forEach(pmid -> {
            counter1.addAndGet(1);
            JedisUtil.set(pmid.getBytes(), "1".getBytes());
            System.out.println("计数 ： " + counter1);
        });

    }


    public void wxArticleAddFilePath(long startTime, long endTime,int startHour,int endHour) throws ConfigurationException, ParseException {
        AtomicLong counter = new AtomicLong(0);
        LinkedList<Document> docList = new LinkedList<>();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        for (long time = startTime; time <= endTime; time++) { //start<=end 相当于while(true)
            if (new Tools().isValidDate(String.valueOf(time))) {
                for (int i = startHour; i <= endHour; i++) {
                    String timeFlag = String.valueOf(i);
                    if (i >= 0 && i < 10) {
                        timeFlag = 0 + timeFlag;
                    }
                    long start_time = format.parse(time + timeFlag + "0000").getTime();
                    long end_time = format.parse(time + timeFlag + "5959").getTime();
                    //下周 改成  addTIme 在 11月23日  0点到11点23分之间    postTime也是 0点到11点23分之间
                    FindIterable<Document> docs = wxAservice.getWxArticleCollect().find(Filters.and(Filters.gte("postTime", start_time), Filters.lte("postTime", end_time),Filters.gte("updateTime", start_time), Filters.lte("updateTime", end_time)));
                    for (Document doc : docs) {
                        counter.addAndGet(1);
                        docList.add(doc);
                        if (docList.size() >= 200) {
                            addFilePathToMongo(docList);
                            try {
                                Thread.sleep(300);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            docList.clear();
                            System.out.println(String.format("%s已处理%s个文章,posttime：%s", DateUtil.format(new Date().getTime(), "yyyy-MM-dd HH:mm:ss"), counter, time + timeFlag));
                        }
                    }
                    if (CollectionUtils.isEmpty(docList)) {
                        continue;
                    }
                }
            }
        }
        addFilePathToMongo(docList);
        docList.clear();
        System.out.println(String.format("%s已处理%s个文章", DateUtil.format(new Date().getTime(), "yyyy-MM-dd HH:mm:ss"), counter));
    }


    private void addFilePathToMongo(LinkedList<Document> docList) {
        long startTime = System.currentTimeMillis(); //排序前取得当前时间
        ArrayList<GWxArticleBasic> gWxArticleBasics = new ArrayList<>();
        LinkedList<Document> docs = new LinkedList<>(docList);
        List<Map<String,String>> list = new ArrayList<Map<String,String>>();
        String content = "";
        String time = "00000000";
        int hour = 0;
        String wxcontent = "";
        String resultDir = "";
        String filename = "";
        StringBuffer toSaveDir = new StringBuffer();
        StringBuffer toProStr = new StringBuffer();
        GWxArticleBasic wxArticleBasic;
        HashMap<String,String> map;

        SimpleDateFormat formater = new SimpleDateFormat("yyyyMMddHH", Locale.CHINA);
        for (Document doc : docs) {
            wxArticleBasic = new GWxArticleBasic();
            if (StringUtils.isNotEmpty(doc.getString("content"))) {
                content = doc.getString("content");
            }
            if (doc.getLong("postTime") != null && doc.getLong("postTime") != 0) {
                String postTime = formater.format(doc.getLong("postTime"));
                time = postTime.substring(0, 8);
                hour = Integer.valueOf(postTime.substring(8, 10));
            }
            resultDir = wxFtpPath.concat(toSaveDir.append(time).append("/").append(hour).append("/").toString());
            filename = md5Password(doc.getString("_id"));
            wxArticleBasic.setFilePath(String.valueOf(resultDir.charAt(0)).equals(".") ? resultDir.substring(1, resultDir.length()).concat(filename) : resultDir.concat(filename));
            // 文件路径:::正文:::ftpclient的key:::文件名称
            map = new HashMap<>();
            map.put("resultDir",resultDir);
            map.put("content",content);
            map.put("key",time + hour);
            map.put("filename",filename);
            list.add(map);
            wxArticleBasic.setArticleUrl(doc.getString("_id"));
            wxArticleBasic.setPmid(doc.getString("pmid"));
            if (StringUtils.isNotEmpty(doc.getString("content"))) {
                wxcontent = Jsoup.parseBodyFragment(doc.getString("content")).text();
            } else {
                wxcontent = "";
            }
            wxArticleBasic.setContent(wxcontent.length() >= 200 ? wxcontent.substring(0, 200) : wxcontent);
            gWxArticleBasics.add(wxArticleBasic);

            //回收再利用
            toSaveDir.delete(0, toSaveDir.length());
            toProStr.delete(0, toProStr.length());
        }
        //批量上传FTP
        uploadFileToFTPWithDirBatchNew(list);
        //更新正文为摘要,加入filepath属性
//        iGaia.addWxArticleBasicBatch(gWxArticleBasics);
        long EndTime = System.currentTimeMillis(); //排序后取得当前时间
        System.out.println("处理文章：" + list.size() + ", 耗时: " + (EndTime - startTime));
        docList.clear();
        list.clear();
        gWxArticleBasics.clear();
    }


    public void handleMedias() throws ConfigurationException {
        AtomicLong counter = new AtomicLong(0);
        LinkedList<Document> docList = new LinkedList<>();

        FindIterable<Document> docs = wxAservice.getWxMediaCollect().find(Filters.eq("isUsed", 2));

        for (Document doc : docs) {
            counter.addAndGet(1);
            docList.add(doc);
            if (docList.size() >= 5000) {
                LinkedList<Document> threadList = new LinkedList<>(docList);
                executor.execute(() -> wxAservice.getWxMediaCollect().insertMany(threadList));
                docList.clear();
                System.out.println(String.format("已导出%s个媒体", counter));
            }
        }

        //收尾
        LinkedList<Document> lastList = new LinkedList<>(docList);
        executor.execute(() -> wxAservice.getWxMediaCollect().insertMany(lastList));
        System.out.println(String.format("已导出%s个媒体", counter));
        executor.shutdown();
        ExecutorStatus(executor);
        System.out.println("!!!!!" + MongoUtils1.instance().getDatabase("gaia2").getCollection("wxMedia").count(Filters.eq("isUsed", 2)));
    }


    /**
     * @author zhangxun
     * 20161201 19:07
     * @description 查询预生产mongo数据库中在文章库中存在，但是文章对应的媒体在媒体库不存在的文章
     */
    public void articlePmidNotInMediaMongoWx() throws Exception{
        AtomicLong counter = new AtomicLong(0);

        HashSet<String> result = new HashSet<String>();
//        FindIterable<Document> docs = wxAservice.getWxMediaCollect().find(Filters.eq("isUsed", 2));
        FindIterable<Document> wxArtileDocs = wxAservice.getWxArticleCollect().find().projection(new Document().append("pmid",1));
        HashSet<String> wxArticlePmid = new HashSet<String>();
        for (Document doc : wxArtileDocs){
            wxArticlePmid.add(doc.getString("pmid"));
        }
        System.out.println("去重后微信文章的pmid的数量为："+wxArticlePmid.size());
        HashSet<String> pmidSet = new HashSet<String>();
        for (String pMid:wxArticlePmid){
            counter.addAndGet(1);
            pmidSet.add(pMid);
            if(pmidSet.size()>=10000){
                executor.execute(() ->{
                    for(String pm:pmidSet){
                        long i = wxAservice.getWxMediaCollect().count(Filters.eq("_id",pm));
                        if(i==0){
                            result.add(pm);
                            System.out.println(pm);
                        }
                    }
                });
                pmidSet.clear();
                System.out.println(String.format("已经执行了%s个媒体", counter));
            }
        }

        executor.execute(() ->{
            for(String pm:pmidSet){
                long i = wxAservice.getWxMediaCollect().count(Filters.eq("_id",pm));
                if(i==0){
                    result.add(pm);
                    System.out.println(pm);
                }
            }
        });
        System.out.println(String.format("已经执行了%s个媒体", counter));

        executor.shutdown();
        ExecutorStatus(executor);
        System.out.println(String.format("微信文章库对应的pmid在微博媒体库中查询是否存在，查询完成,一共有%个媒体在库中查不到",result.size()));
        System.out.println("微信查询不到的媒体为:"+result);


    }
    /**
     * @author zhangxun
     * 20161201 19:07
     * @description 查询预生产mongo数据库中在文章库中存在，但是文章对应的媒体在媒体库不存在的文章
     */
    public void articlePmidNotInMediaMongoWb() throws Exception{
        AtomicLong counter = new AtomicLong(0);

        HashSet<String> result = new HashSet<String>();
//        FindIterable<Document> docs = wxAservice.getWxMediaCollect().find(Filters.eq("isUsed", 2));
        FindIterable<Document> wbArtileDocs = wbAservice.getWbArticleCollect().find().projection(new Document().append("pmid",1));
        HashSet<String> wbArticlePmid = new HashSet<String>();
        for (Document doc : wbArtileDocs){
            wbArticlePmid.add(doc.getString("pmid"));
        }
        System.out.println("去重后微博文章的pmid的数量为："+wbArticlePmid.size());
        HashSet<String> pmidSet = new HashSet<String>();
        for (String pMid:wbArticlePmid){
            counter.addAndGet(1);
            pmidSet.add(pMid);
            if(pmidSet.size()>=10000){
                executor.execute(() ->{
                    for(String pm:pmidSet){
                        long i = wbAservice.getWbMediaCollect().count(Filters.eq("_id",pm));
                        if(i==0){
                            result.add(pm);
                            System.out.println(pm);
                        }
                    }
                });
                pmidSet.clear();
                System.out.println(String.format("已经执行了%s个媒体", counter));
            }
        }

        executor.execute(() ->{
            for(String pm:pmidSet){
                long i = wxAservice.getWxMediaCollect().count(Filters.eq("_id",pm));
                if(i==0){
                    result.add(pm);
                    System.out.println(pm);
                }
            }
        });
        System.out.println(String.format("已经执行了%s个媒体", counter));

        executor.shutdown();
        ExecutorStatus(executor);
        System.out.println(String.format("微博文章库对应的pmid在微博媒体库中查询是否存在，查询完成,一共有%个媒体在库中查不到",result.size()));
        System.out.println("微博查询不到的媒体为:"+result);


    }



    static AtomicLong counter = new AtomicLong(0);

    public void handleArticles(Long time, Long timeEnd) {
        LinkedList<String> docList = new LinkedList<>();
        //查询新collection中的媒体pmid
        FindIterable<Document> mDocs = wxAservice.getWxMediaCollect().find().projection(new Document().append("_id", 1));
        for (Document mDoc : mDocs) {
            docList.add(mDoc.getString("_id"));
            if (docList.size() >= 10) {
                insertArticles(docList, time, timeEnd);
                docList.clear();
            }
        }
        //收尾
        insertArticles(docList, time, timeEnd);
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
                Thread.sleep(8000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void insertArticles(LinkedList<String> docList, Long time, Long timeEnd) {
        LinkedList<String> pmids = new LinkedList<>(docList);
        executor.execute(() -> {
            //媒体文章
            FindIterable<Document> aDocs = null;
            try {
                aDocs = MongoUtils1.instance().getDatabase("gaia2").getCollection("wxArticle").find(Filters.and(Filters.in("pmid", pmids), Filters.gt("postTime", time), Filters.lt("postTime", timeEnd)));
            } catch (ConfigurationException e) {
                e.printStackTrace();
            }
            LinkedList<Document> aList = new LinkedList<>();
            for (Document aDoc : aDocs) {
                aList.add(aDoc);
                counter.addAndGet(1);
            }
            if (aList.isEmpty()) return;
            wxAservice.getWxArticleCollect().insertMany(aList);
            System.out.println(String.format("已导出%s篇文章", counter));
        });
    }

    public Long getTime(String pattern, int days) {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        try {
            calendar.setTime(sdf.parse(sdf.format(new Date())));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        calendar.add(Calendar.DAY_OF_YEAR, days);
        return calendar.getTimeInMillis();
    }


    public void test() throws ConfigurationException {
        AtomicLong counter = new AtomicLong(0);
        LinkedList<Integer> docList = new LinkedList<>();
        List<Integer> list = new ArrayList<>();
        for (int i = 1602300; i > 0; i--) {

            list.add(i);

        }

        for (int doc : list) {
            counter.addAndGet(1);
            docList.add(doc);
            if (docList.size() >= 5000) {
                LinkedList<Integer> threadList = new LinkedList<>(docList);
                executor.execute(() -> System.out.println("@@@:" + threadList.size()));
                docList.clear();
            }
        }

        //收尾
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("!!!!!!!!" + docList.size());
        LinkedList<Integer> lastList = new LinkedList<>(docList);
        executor.execute(() -> System.out.println("@@@:" + lastList.size()));
        System.out.println(String.format("已导出%s个媒体", counter));
        executor.shutdown();
        ExecutorStatus(executor);
    }


    public static void main(String[] args) throws ConfigurationException {

        new WxMediaClean().test();

    }

}
