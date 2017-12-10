package com.ptb.gaia.tool.esTool.convert;

import com.ptb.gaia.tool.esTool.data.EsData;
import com.ptb.gaia.tool.esTool.data.MongoData;
import com.ptb.gaia.tool.esTool.data.MysqlData;
import com.ptb.gaia.tool.esTool.util.EsTransportClient;
import com.ptb.gaia.tool.esTool.util.MongoClientm;
import com.ptb.gaia.tool.esTool.util.MysqlClientm;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


/**
 * Created by watson zhang on 16/7/27.
 */
public class AddTimesEntry {
    private static final Logger logger = LoggerFactory.getLogger(AddTimesEntry.class);
    public EsData esData;
    public EsTransportClient esClient;
    public MysqlData mysqlData;
    public MysqlClientm mysqlClientm;
    public MongoData mongoData;
    public MongoClientm mongo;

    public AddTimesEntry(){
        mysqlClientm = new MysqlClientm();
        mysqlData = mysqlClientm.getMysqlData();

        esClient = new EsTransportClient();
        esData = esClient.getEsData();

        mongo = new MongoClientm();
        mongoData = mongo.getMongoData();
    }

    public void updateAddTimes(){
        long totalNum = 0;
        long internalTime = 60000;
        int timeMinute = 0;
        List<HashMap<String, String>> rs;
        String sql = String.format("SELECT `type`,`p_mid`, count(*) as cnt FROM %s group by p_mid", mysqlData.getMysqlTableName());
        rs = mysqlClientm.result(sql);

        long startTime = System.currentTimeMillis();
        Iterator<HashMap<String, String>> iterator = rs.iterator();
        while (iterator.hasNext()){
            HashMap<String, String> nextIter = iterator.next();
            try {
                XContentBuilder builder = JsonXContent.contentBuilder().startObject()
                        .field("addTimes", nextIter.get("cnt"))
                        .endObject();

                if (nextIter.get("type").equals("1")){
                    esClient.weixinUpsert("pmid", nextIter.get("p_mid"), builder);
                } else if (nextIter.get("type").equals("2")){
                    esClient.weiboUpsert("pmid", nextIter.get("p_mid"), builder);
                }
                long endTime = System.currentTimeMillis();
                totalNum++;
                if ((endTime - startTime)/internalTime  > timeMinute){
                    timeMinute++;
                    logger.info("current total num: {}", totalNum);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("total num: {}", totalNum);
    }

    public static void importAddTimes(){
        AddTimesEntry entry = new AddTimesEntry();
        entry.updateAddTimes();
    }

    public static void main(String[] args){
        //AddTimesEntry entry = new AddTimesEntry();
        //entry.updateAddTimes();
        String ss="abcd";
        ss.substring(2);
        System.out.println(ss);
    }
}
