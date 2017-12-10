package com.ptb.gaia.tool.esTool.util;

import com.alibaba.fastjson.JSON;
import com.ptb.gaia.tool.esTool.convert.EsMediaConvert;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.bson.Document;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;


/**
 * @DESC:
 * @VERSION:
 * @author: xuefeng
 * @Date: 2016/8/1
 * @Time: 16:16
 */
public class MediaTags {
    private EsMediaConvert esMediaConvert = new EsMediaConvert();
    private EsTransportClient client = new EsTransportClient();
    private String indexName;

    void initConfig() throws ConfigurationException {
        Configuration configuration = new PropertiesConfiguration("ptb.properties");
        indexName = configuration.getString("gaia.es.media.index");
    }

    public MediaTags() {
        try {
            initConfig();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    public void importTags(String path) throws IOException {
        Files.lines(Paths.get(path)).parallel().map(line -> JSON.parseObject(line, Document.class)).filter(doc -> doc.get("tags") != null).forEach(doc -> {
            if (doc.getInteger("plat") == 1) {
                //XContentBuilder weixinMediaData = esMediaConvert.getWeixinMediaData(doc);
                //if (weixinMediaData != null)
                //    client.insert(indexName, "weixinMedia", doc.getString("pmid"), weixinMediaData);
            }
            if (doc.getInteger("plat") == 2) {
                //XContentBuilder weiboMediaData = esMediaConvert.getWeiboMediaData(doc);
                //if (weiboMediaData != null)
                //    client.insert(indexName, "weiboMedia", doc.getString("pmid"), weiboMediaData);
            }
        });
    }

    public static void main(String[] args) throws ConfigurationException, IOException {
      /*  if (args.length != 1) {
            System.err.println("输入参数错误");
            System.exit(1);
        }*/
        new MediaTags().importTags("g:/ret.json");
    }
}
