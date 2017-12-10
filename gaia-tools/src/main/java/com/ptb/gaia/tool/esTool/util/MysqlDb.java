package com.ptb.gaia.tool.esTool.util;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2016/9/6 0006.
 */
public class MysqlDb {
    private static Logger logger = LoggerFactory.getLogger(MysqlDb.class);
    public static final String name = "com.mysql.jdbc.Driver";
    public Connection conn = null;
    public PreparedStatement pst = null;
    public ResultSet rs =null;
    public MysqlDb(){

    }


    public MysqlDb(String sql) {
        try {
            PropertiesConfiguration conf = new PropertiesConfiguration("ptb.properties");
            Class.forName(name);
            conn = DriverManager.getConnection(conf.getString("spring.datasource.url"), conf.getString("spring.datasource.username"), conf.getString("spring.datasource.password"));
            pst = conn.prepareStatement(sql);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("error myslq  Abnormal links ");
        }
    }

    public List<Map<String,String>> getLostMediaList(String sql){
        List<Map<String,String>> list = new ArrayList<>();
        MysqlDb mysqlDb = new MysqlDb(sql);
        try{
            rs = mysqlDb.pst.executeQuery();
            while (rs.next()){
                Map<String,String> map = new HashedMap();
                map.put("id",rs.getString(1));
                map.put("media_name",rs.getString(2));
                list.add(map);
            }
            rs.close();
            mysqlDb.conn.close();
            mysqlDb.pst.close();
        }catch (Exception e){
            e.printStackTrace();
            logger.info("getLostMediaList errro ");
        }
        return  list;

    }

    public void updateLostMediaStatus(Integer id){
        String sql ="update lost_media set status=1 ,update_time=CURRENT_TIMESTAMP() where id="+id;
        MysqlDb mysqlDb = new MysqlDb(sql);
        try{
            if(mysqlDb.pst.executeUpdate() > 0){
                logger.info("update result success");
            }
            mysqlDb.pst.close();
        }catch (Exception e){
            e.printStackTrace();
            logger.info("updateLostMediaStatus errro ");
        }

    }

}
