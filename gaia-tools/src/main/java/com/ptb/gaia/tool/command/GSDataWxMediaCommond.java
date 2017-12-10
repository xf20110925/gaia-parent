package com.ptb.gaia.tool.command;

import com.alibaba.fastjson.JSON;
import com.jayway.jsonpath.JsonPath;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.ptb.gaia.service.utils.MongoUtils;
import com.ptb.gaia.tool.utils.MysqlClient;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @DESC:
 * @VERSION:
 * @author: xuefeng
 * @Date: 2017/1/12
 * @Time: 18:21
 */
public class GSDataWxMediaCommond {
    public static class GSWxMedia {
        private String wx_nickname;
        private String wx_name;
        private String readnum_all;
        private String likenum_all;

        public GSWxMedia() {
        }

        public GSWxMedia(String wx_nickname, String wx_name, String readnum_all, String likenum_all) {
            this.wx_nickname = wx_nickname;
            this.wx_name = wx_name;
            this.readnum_all = readnum_all;
            this.likenum_all = likenum_all;
        }

        public String getWx_nickname() {
            return wx_nickname;
        }

        public void setWx_nickname(String wx_nickname) {
            this.wx_nickname = wx_nickname;
        }

        public String getWx_name() {
            return wx_name;
        }

        public void setWx_name(String wx_name) {
            this.wx_name = wx_name;
        }

        public String getReadnum_all() {
            return readnum_all;
        }

        public void setReadnum_all(String readnum_all) {
            this.readnum_all = readnum_all;
        }

        public String getLikenum_all() {
            return likenum_all;
        }

        public void setLikenum_all(String likenum_all) {
            this.likenum_all = likenum_all;
        }

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null) return false;
            GSWxMedia gsWxMedia = (GSWxMedia) o;
            if (wx_name.equals(gsWxMedia.wx_name)) return true;
            return false;
        }

        @Override
        public int hashCode() {
            int result = wx_name.hashCode() * 31;
            return result;
        }
    }


    static PropertiesConfiguration conf = null;

    static {
        try {
            conf = new PropertiesConfiguration("ptb.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        String[] mongodbHostArray = conf.getStringArray("gaia.service.mongodb.host");
        int mongodbPort = conf.getInt("gaia.service.mongodb.port");
        MongoUtils.configure(mongodbHostArray, mongodbPort);
    }

    public static Set<String> readFromCatalog(String catalog) throws IOException {
        //读取目录下的所有文件并加载到内存
        Set<String> wxIds = Files.list(Paths.get(catalog)).flatMap(path -> {
            try {
                return Files.lines(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return Stream.empty();
        }).flatMap(line -> Arrays.stream(line.split("-->")[1].replaceAll("[\\s+\\[\\]]", "").split(","))).filter(StringUtils::isNoneEmpty).collect(Collectors.toSet());
        System.out.println(wxIds.size());
        System.out.println(wxIds);
        return wxIds;
    }

    public static Map<String, Set<GSWxMedia>> stateWxIdNotInMongo(String readPath, String writePath) throws IOException {
        //读取目录下的所有文件并加载到内存
        Map<String, Set<GSWxMedia>> ret = Files.list(Paths.get(readPath)).flatMap(path -> {
            try {
                return Files.lines(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return Stream.empty();
        }).collect(Collectors.toMap(line -> line.split("-->")[0], line -> exceptWxIdsByMongo(JSON.parseArray(line.split("-->")[1], GSWxMedia.class).stream().collect(Collectors.toSet()))));
        ret.forEach((k, v) -> {
            try {
                if (!Files.exists(Paths.get(writePath))) {
                    Files.createDirectories(Paths.get(writePath).getParent());
                    Files.createFile(Paths.get(writePath));
                }
                Files.write(Paths.get(writePath), String.format("%s-->%s%s", k, v, "\r\n").getBytes(), StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return ret;
    }

    /**
     * 统计mongo中不存在的微信id
     *
     * @param gsWxMedias
     * @return
     */
    public static Set<GSWxMedia> exceptWxIdsByMongo(Set<GSWxMedia> gsWxMedias) {
        String dbName = conf.getString("gaia.tools.mongo.dbName");
        FindIterable<Document> docs = MongoUtils.instance().getDatabase(dbName).getCollection("wxMedia").find(Filters.in("weixinId", gsWxMedias.stream().map(GSWxMedia::getWx_name).collect(Collectors.toSet()))).projection(new Document("weixinId", 1));
        HashSet<String> mongWxIds = new HashSet<>();
        for (Document doc : docs) {
            String weixinId = doc.getString("weixinId");
            mongWxIds.add(weixinId);
        }
        gsWxMedias.removeIf(gsWxMedia -> mongWxIds.contains(gsWxMedia.getWx_name()));
        return gsWxMedias;
    }

    private static Set<String> convertWxId2Pmid(List<String> wxIds) {
        Set<String> pmids = new HashSet<>();
        FindIterable<Document> documents = MongoUtils.instance().getDatabase(conf.getString("gaia.tools.mongo.dbName")).getCollection("wxMedia").find(Filters.in("weixinId", wxIds)).projection(new Document("_id", 1));
        for (Document document : documents) {
            pmids.add(document.getString("_id"));
        }
        return pmids;
    }

    private static Map<String, String> convertWxIds2Pmids(List<String> wxIds) {
        Map<String, String> wxidPmids = new HashMap<>();
        FindIterable<Document> documents = MongoUtils.instance().getDatabase(conf.getString("gaia.tools.mongo.dbName")).getCollection("wxMedia").find(Filters.in("weixinId", wxIds)).projection(new Document("_id", 1).append("weixinId", 1));
        for (Document document : documents) {
            wxidPmids.put(document.getString("_id"), document.getString("weixinId"));
        }
        return wxidPmids;
    }

    public static void convert() {
        String sql = "select weixin_id from wx_media_id_pmid";
        List<String> wxIds = MysqlClient.instance.queryForList(sql, String.class);
        Map<String, String> map = convertWxIds2Pmids(wxIds);
        String updateSql = "update wx_media_id_pmid set pmid='%s' where weixin_id='%s'";
        for (Map.Entry<String, String> entry : map.entrySet()) {
            MysqlClient.instance.template.update(String.format(updateSql, entry.getKey(), entry.getValue()));
        }

    }

    public static void write2File(String readFilePath, String writeFilePath) throws IOException {
        Path catalog = Paths.get(writeFilePath).getParent();
        if (!Files.exists(catalog)) Files.createDirectories(catalog);
        if (!Files.exists(Paths.get(writeFilePath))) Files.createFile(Paths.get(writeFilePath));
        Path path = Paths.get(readFilePath);
        Files.lines(path, StandardCharsets.UTF_8).forEach(line -> {
            String[] split = line.replaceAll("(\\s+|\\[|\\])", "").split("-->");
            List<String> wxIds = Arrays.stream(split[1].split(",")).collect(Collectors.toList());
            Set<String> pmids = convertWxId2Pmid(wxIds);
            try {
                Files.write(Paths.get(writeFilePath), String.format("%s-->%s%s", split[0], pmids, "\r\n").getBytes(), StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

    }

    private static Map<String, String> getGsClassifyFromysql() {
        String sql = "select classify_name, i_id from media_classify_gs";
        List<Map<String, Object>> classifyNames = MysqlClient.instance.queryForList(sql);
        Map<String, String> collect = classifyNames.stream().collect(Collectors.toMap(map -> map.get("classify_name").toString(), map -> map.get("i_id").toString()));
        return collect;
    }

    public static Map<String, List<String>> readFromCatalog1(String catalog) throws IOException {
        Map<String, String> classifyNames = getGsClassifyFromysql();
        //读取目录下的所有文件并加载到内存
        Map<String, List<String>> ret = Files.list(Paths.get(catalog)).flatMap(path -> {
            try {
                return Files.lines(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return Stream.empty();
        }).filter(line -> classifyNames.containsKey(line.split("-->")[0])).collect(Collectors.toMap(line -> classifyNames.get(line.split("-->")[0]),
                line -> JSON.parseArray(JsonPath.parse(line.split("-->")[1]).read("$..wx_name").toString(), String.class), (v1, v2) -> {
                    v1.addAll(v2);
                    return v1;
                }));
        return ret;
    }

    public static Map<String, List<String>> convertWxId2Pmid(String catalog) throws IOException {
        Map<String, List<String>> map = readFromCatalog1(catalog);
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            entry.setValue(new ArrayList<>(convertWxId2Pmid(entry.getValue())));
        }
        return map;
    }

    public static void main(String[] args) throws IOException {
//        Map<String, List<String>> pmids = convertWxId2Pmid("F:\\gsdata");
        convert();
    }
}
