package com.ptb.gaia.etl.flume.utils;

import com.ptb.gaia.etl.flume.entity.ArticleBrandBean;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.assertj.core.util.Strings;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by MengChen on 2017/3/15.
 */
public class MysqlConnect {

    private Connection conn;
    private Statement statement;
    private static String url = "";
    private static String userName = "";
    private static String password = "";

    private MysqlConnect() {
        try {
            Configuration conf = new PropertiesConfiguration("ptb.properties");
            url = conf.getString("spring.datasource.url", "jdbc:mysql://192.168.40.15:3306/zeus?useUnicode=true&characterEncoding=utf8");
            userName = conf.getString("spring.datasource.username", "developer");
            password = conf.getString("spring.datasource.password", "ptb-yanfa-users");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            this.conn = DriverManager.getConnection(url, userName, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class Nested {
        private static MysqlConnect instance = new MysqlConnect();
    }


    public static MysqlConnect getInstance() {
        return Nested.instance;
    }

    public ResultSet query(String query) throws SQLException {
        statement = MysqlConnect.getInstance().conn.createStatement();
        return statement.executeQuery(query);
    }

    public void insertBatch(List<ArticleBrandBean> list) throws SQLException {
        conn.setAutoCommit(false);
        PreparedStatement InsertStatement = conn.prepareStatement("insert into zeus.article_brand (url,pmid,brand_id,actual_url,add_time) values (?,?,?,?,?)");
        InsertStatement.clearBatch();
        list.forEach(x -> {
            try {
                InsertStatement.setString(1, x.getUrl());
                InsertStatement.setString(2, x.getPmid());
                InsertStatement.setLong(3, x.getBrandId());
                InsertStatement.setString(4, x.getActualUrl());
                InsertStatement.setString(5, x.getTime());
                InsertStatement.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        InsertStatement.executeBatch();
        conn.commit();
    }

    public Map<Long, List<String>> queryKeyWords(String query) throws SQLException {
        statement = MysqlConnect.getInstance().conn.createStatement();
        Long id;
        List<String> list;
        ResultSet resultSet = statement.executeQuery(query);
        Map dirMap = new HashMap<Long, ArrayList<String>>();
        while (resultSet.next()) {
            id = resultSet.getLong(1);
            list = new ArrayList<>(Arrays.asList(resultSet.getString(2).split(",")));
            dirMap.put(id, list.stream().map(String::toLowerCase)
                    .collect(Collectors.toList()));
        }
        return dirMap;
    }

    public Map<String, Long> queryDir(String query) throws SQLException {
        statement = MysqlConnect.getInstance().conn.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        Map dirMap = new HashMap<String, Long>();
        while (resultSet.next()) {
            dirMap.put(resultSet.getString(1), resultSet.getLong(2));
        }

        return dirMap;
    }

    public Map<Long, List<Long>> queryCategoryBrandRel(String query) throws SQLException {
        statement = MysqlConnect.getInstance().conn.createStatement();
        Long id;
        List<String> list;
        ResultSet resultSet = statement.executeQuery(query);
        Map dirMap = new HashMap<Long, ArrayList<Long>>();
        while (resultSet.next()) {
            id = resultSet.getLong(1);
            list = new ArrayList<>(Arrays.asList(resultSet.getString(2).split(",")));
            dirMap.put(id, list.stream().map(Long::valueOf)
                    .collect(Collectors.toList()));
        }
        return dirMap;
    }


    public static void main(String[] args) throws SQLException, ConfigurationException {
        //测试mysql
//        Map resultSet = MysqlConnect.getInstance().queryCategoryBrandRel("select t.category_id,GROUP_CONCAT(t.brand_id) as brand_id from category_brand_rel t group by t.category_id ;");
        //测试mongodb
//        GWxArticleService gWxArticleService = new GWxArticleService();
//        FindIterable<Document> findIterable1 = gWxArticleService.getWxMediaCollect().find(Filters.eq("_id", "MjM5NTUzNTg0NA==")).projection(new Document().append("tags", 1)).limit(1).noCursorTimeout(true);
//        for (Document document1 : findIterable1) {
//            JedisUtil.set("brand_".concat(document1.getString("_id")).getBytes(), document1.get("tags", List.class).get(0).toString().getBytes());
//        }
//
//        System.out.println(JedisUtil.get("brand_MjM5NTUzNTg0NA=="));

    }

}
