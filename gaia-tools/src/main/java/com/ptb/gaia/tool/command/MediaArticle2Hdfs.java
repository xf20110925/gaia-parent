package com.ptb.gaia.tool.command;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.ptb.gaia.search.utils.EsUtils;
import com.ptb.gaia.service.utils.MongoUtils;
import com.ptb.gaia.tool.utils.MysqlClient;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bson.Document;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.seleniumhq.jetty7.util.security.Credential;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

/**
 * @DESC:
 * @VERSION:
 * @author: xuefeng
 * @Date: 2017/1/3
 * @Time: 15:32
 */
public class MediaArticle2Hdfs {

    static PropertiesConfiguration conf;

    public enum ESUtil {
        instance;
        private Client client;

        public void initClient() {
            client = EsUtils.getCli();
        }

        ESUtil() {
            initClient();
        }

        public Set<String> get(Set<String> urls) {
            List<String> md5urls = urls.stream().map(url -> Credential.MD5.digest(url).substring(4)).collect(Collectors.toList());
            QueryBuilder qb = termsQuery("_id", md5urls);
            SearchResponse response = client.prepareSearch(conf.getString("gaia.es.article.content.index", "article_content")).setTypes("wxArticle").setQuery(qb).setFrom(0).setSize(urls.size()).get();
            Set<String> articles = new HashSet<>();
            for (SearchHit searchHit : response.getHits()) {
                Object content = searchHit.getSource().get("content");
                if (content == null) continue;
                articles.add((String) content);
            }
            return articles;
        }

        public static void main(String[] args) {
            HashSet<String> urls = new HashSet<>();
            urls.add("http://mp.weixin.qq.com/s?__biz=MzAwMjQ5Nzg2MA==&mid=2652636734&idx=2&sn=aabc86d133f49d47f474493592c9122b#rd");
            urls.add("http://mp.weixin.qq.com/s?__biz=MzA4MDU5ODUxMQ==&mid=2650329669&idx=1&sn=c07ce44287bd7aec06866dfa6d4479eb#rd");
            Set<String> contents = ESUtil.instance.get(urls);
            System.out.println(contents.size());
        }

    }

    static {
        try {
            conf = new PropertiesConfiguration("ptb.properties");
            String[] mongodbHostArray = conf.getStringArray("gaia.service.mongodb.host");
            int mongodbPort = conf.getInt("gaia.service.mongodb.port");
            MongoUtils.configure(mongodbHostArray, mongodbPort);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param postTime    发文时间
     * @param limit       最大条数
     * @param fileCatalog 文件目录
     * @param overwrite   是否覆盖原文件
     */
    public static final String uploadDate = new SimpleDateFormat("yyyyMMdd").format(new Date());
    public static void save(long postTime, int limit, String fileCatalog, boolean overwrite, String typeId) {
        String fileName = String.format("%s%s-%s.txt", fileCatalog, typeId, uploadDate);
        String dbName = conf.getString("gaia.tools.mongo.dbName");
        List<String> pmids = getPmids(typeId);
        if (pmids.isEmpty()) {
            System.out.println(String.format("%s类型下媒体个数为零", typeId));
            return;
        }
        List<String> contents = pmids.stream().flatMap(pmid -> {
            Set<String> urls = getArticleUrls(dbName, pmid, postTime, limit);
            return ESUtil.instance.get(urls).stream().
                    filter(content ->
                            content.length() >= 200).
                    map(content ->
                            content.replaceAll("\\s+", ""));
        }).collect(Collectors.toList());
        if (contents.isEmpty()) {
            System.out.println("从ES查询文章内容为空");
            return;
        }
        try {
            writeHdfs(typeId, fileName, overwrite, contents);
            System.out.println(String.format("write %s -> %d 篇文章", typeId, contents.size()));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("保存失败！");
        }
    }

    public static List<String> getPmids(String typeId) {
        String sql = "select pmid from media_model where category = ? ";
        List<String> pmids = MysqlClient.instance.queryForList(sql, String.class, typeId);
        return pmids;
    }

    /**
     * 从mongo获取数据
     */
    private static Set<String> getArticleUrls(String dbName, String pmid, long postTime, int limit) {
        FindIterable<Document> docs = MongoUtils.instance().getDatabase(dbName).getCollection("wxArticle").find(Filters.and(Filters.eq("pmid", pmid), Filters.gte("postTime", postTime))).limit(limit).projection(new Document("_id", 1)).sort(new Document("postTime", -1));
        Set<String> urls = new HashSet<>();
        for (Document doc : docs) {
            urls.add(doc.getString("_id"));
        }
        return urls;
    }

    private static void writeHdfs(String typeId, String fileName, boolean overwrite, List<String> contents) throws IOException {
        FSDataOutputStream out = null;
        try {
            Configuration hdfsConf = new Configuration();
            hdfsConf.set("fs.defaultFS", conf.getString("gaia.tools.hdfs", "hdfs://192.168.200.156:8020"));
            FileSystem fs = FileSystem.get(hdfsConf);
            Path outFile = new Path(fileName);
            if (!fs.exists(outFile)) {
                out = fs.create(outFile);
            } else {
                if (overwrite) out = fs.create(outFile, overwrite);
                else out = fs.append(new Path(fileName));
            }
            for (String content : contents) {
                out.write(String.format("%s\t%s, %s", typeId, content, "\r\n").getBytes());
            }
        } finally {
            out.close();
        }
    }

    public static void insert2MediaModel(String filePath) throws IOException {
        Files.lines(Paths.get(filePath)).forEach(line -> {
            String[] split = line.replaceAll("\\s\\[\\]", "").split("-->");
            String typeId = split[0];
            List<String> pmids = Arrays.asList(split[1].split(","));
            String sql = "insert into media_model(pmid, category) values(?, ?)";
            MysqlClient.instance.template.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    ps.setString(1, pmids.get(i).trim());
                    ps.setString(2, typeId);
                }

                @Override
                public int getBatchSize() {
                    return pmids.size();
                }
            });
        });
    }

    public static void insert2MediaModelV1(String catalog) throws IOException {
        Map<String, List<String>> map = GSDataWxMediaCommond.convertWxId2Pmid(catalog);
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            String sql = "insert into media_model(pmid, category) values(?, ?)";
            MysqlClient.instance.template.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    ps.setString(1, entry.getValue().get(i).trim());
                    ps.setString(2, entry.getKey());
                }

                @Override
                public int getBatchSize() {
                    return entry.getValue().size();
                }
            });
        }
    }

    public static void main(String[] args) throws IOException {
//        save(0, 100, "/user/gaia/test_hive/", true, "1001000000");
    /*SearchResponse response 	int i = 0;
= ESUtil.instance.client.prepareSearch("article_test_2").setTypes("wxArticle").setSize(200).get();
    SearchHits searchHits = response.getHits();
	for (SearchHit searchHit : searchHits) {
	  i++;
	  String pmid = (String)searchHit.getSource().get("pmid");
	  String sql = String.format("insert into media_model(pmid, category) values ('%s', 1001000000)", pmid);
	  MysqlClient.instance.template.execute(sql);
	}
	System.out.println(i);*/
        insert2MediaModelV1("F:\\gsdata");
    }
}
