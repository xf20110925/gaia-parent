package com.ptb.gaia.tool.utils;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by watson zhang on 16/7/27.
 */
public class MysqlUtil {
    PropertiesConfiguration conf;
    private static Logger logger = LoggerFactory.getLogger(MysqlUtil.class);
    private final String driver = "com.mysql.cj.jdbc.Driver";
    private Connection conn = null;
    private Statement stmt = null;

    MysqlData mysqlData;

    public MysqlUtil() {
        mysqlData = new MysqlData();
        try {
            conf = new PropertiesConfiguration("ptb.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        mysqlData.setMysqlHost(conf.getString("gaia.tools.mysql.host", "192.168.5.50:3306/anny_web"));
        mysqlData.setMysqlUser(conf.getString("gaia.tools.mysql.user", "client"));
        mysqlData.setMysqlPwd(conf.getString("gaia.tools.mysql.pwd", "123456"));
        mysqlData.setMysqlTableName(conf.getString("athene.mysql.tableName", "user_media"));
    }

    public MysqlData getMysqlData() {
        return mysqlData;
    }

    public void close() {
        try {
            if (stmt != null) {
                stmt.close();
                stmt = null;
            }
            if (conn != null) {
                conn.close();
                conn = null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            stmt = null;
            conn = null;
            logger.error("close error!", e.getNextException());
        }
    }

    public void connectMySQL() {
        try {
            Class.forName(driver).newInstance();
            conn = (Connection) DriverManager.getConnection("jdbc:mysql://"
                    + mysqlData.getMysqlHost() + "?useUnicode=true&characterEncoding=UTF8", mysqlData.getMysqlUser(), mysqlData.getMysqlPwd());
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void statement() {
        if (conn == null) {
            this.connectMySQL();
        }
        try {
            stmt = (Statement) conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public ResultSet resultSet(String sql) {
        ResultSet rs = null;
        if (stmt == null) {
            this.statement();
        }
        try {

            rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("execute error {}", sql);
        }
        return rs;
    }

    public List<HashMap<String, String>> result(String sql) {
        try {
            ResultSet rs = this.resultSet(sql);
            List<HashMap<String, String>> result = new ArrayList<HashMap<String, String>>();
            ResultSetMetaData md = rs.getMetaData();
            int cc = md.getColumnCount();
            while (rs.next()) {
                HashMap<String, String> columnMap = new HashMap<String, String>();
                for (int i = 1; i <= cc; i++) {
                    columnMap.put(md.getColumnName(i), rs.getString(i));
                }
                result.add(columnMap);
            }
            return result;
        } catch (SQLException e) {
            logger.error("connect error!", e.getNextException());
            this.close();
            return null;
        }
    }

    public String getStartId(){
        String start;
        List<HashMap<String, String>> rs;
        String sql = String.format("SELECT `type`,`p_mid`, count(*) as cnt FROM %s group by p_mid", mysqlData.getMysqlTableName());
        rs = this.result(sql);
        start = rs.get(0).get("cnt");
        return start;
    }



    public static void main(String[] args){
        MysqlUtil mysqlUtil = null;
        mysqlUtil = new MysqlUtil();
        mysqlUtil.getStartId();
    }

}
