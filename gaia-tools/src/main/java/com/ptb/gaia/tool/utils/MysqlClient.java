package com.ptb.gaia.tool.utils;

import com.alibaba.fastjson.JSON;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public enum MysqlClient {
    instance;

    private String driverClassName = "com.mysql.jdbc.Driver";
    private String mysqlHost;
    private String mysqlUser;
    private String mysqlPwd;
    public JdbcTemplate template;


    MysqlClient() {
        initConn();
    }

    private void initConn() {
        try {
            PropertiesConfiguration conf = new PropertiesConfiguration("ptb.properties");
//			mysqlHost =  String.format("jdbc:mysql://%s/zeus?useUnicode=true&characterEncoding=utf-8", conf.getString("mysql.address", "192.168.5.50:3306"));
            mysqlHost = String.format("jdbc:mysql://%s?useUnicode=true&characterEncoding=utf8", conf.getString("gaia.tools.mysql.host", "192.168.40.15:3306/tmp"));
            mysqlUser = conf.getString("gaia.tools.mysql.user", "developer");
            mysqlPwd = conf.getString("gaia.tools.mysql.pwd", "ptb-yanfa-users");
            DriverManagerDataSource dataSource = new DriverManagerDataSource();
            dataSource.setDriverClassName(driverClassName);
            dataSource.setUrl(mysqlHost);
            dataSource.setUsername(mysqlUser);
            dataSource.setPassword(mysqlPwd);
            template = new JdbcTemplate(dataSource);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    public <T> List<T> query(String sql, Class<T> clazz, Object... args) {
        List<T> ret = template.queryForList(sql, args).parallelStream().map(obj -> JSON.parseObject(JSON.toJSONString(obj), clazz)).collect(Collectors.toList());
        return ret;
    }

    public <T> List<T> queryForList(String sql, Class<T> clazz, Object... args) {
        return template.queryForList(sql, clazz, args);

    }

    public  List<Map<String, Object>> queryForList(String sql, Object... args) {
        return template.queryForList(sql, args);

    }

    public static void main(String[] args) {
        int size = MysqlClient.instance.template.queryForList("select * from media_tag").size();
        System.out.println(size);
    }
}

