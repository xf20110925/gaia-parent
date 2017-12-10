package com.ptb.gaia.utils;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * Created by watson zhang on 2016/11/24.
 */
public class EsClient {
    static Logger logger  = LoggerFactory.getLogger(EsClient.class);
    static Configuration conf ;
    private static final String[] esHostNames;
    private static final String ptbEsClusterName;
    private static InetSocketTransportAddress[] esServerAddresses;
    private static TransportClient transportClient;
    private static Settings settings;

    private static class ClientHolder {
        private static final TransportClient transportClient = TransportClient.builder().settings(settings).build();
    }

    static {
        try {
            conf = new PropertiesConfiguration("ptb.properties");
            esHostNames = conf.getStringArray("gaia.es.hosts");
            esServerAddresses = parseEsAddressByHostName(esHostNames);
            ptbEsClusterName = conf.getString("gaia.es.clusterName");
            settings = Settings.settingsBuilder()
                    .put("cluster.name", ptbEsClusterName).build();
        } catch (ConfigurationException e) {
            throw new RuntimeException("找不到ptb.properties");
        }
        logger.info("es server:\n" +
                " hosts:{}\n" +
                " cluster: {}",
                esHostNames,
                ptbEsClusterName);
    }

    private EsClient(){}

    public static synchronized TransportClient getCli() {
        logger.info("Using ElasticSearch hostnames: {} ",
                Arrays.toString(esServerAddresses));
        if (transportClient != null){
           return transportClient;
        }
        transportClient = ClientHolder.transportClient;
        for (InetSocketTransportAddress host : esServerAddresses) {
            transportClient.addTransportAddress(host);
        }
        return transportClient;
    }

    private static InetSocketTransportAddress[] parseEsAddressByHostName(String[] hostNames) {
        logger.warn(Arrays.toString(hostNames));
        esServerAddresses = new InetSocketTransportAddress[hostNames.length];
        for (int i = 0; i < hostNames.length; i++) {
            String[] hostPort = hostNames[i].trim().split(":");
            String host = hostPort[0].trim();
            int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim()) : 9300;
            esServerAddresses[i] = new InetSocketTransportAddress(new InetSocketAddress(host, port));
        }
        return esServerAddresses;
    }

    public static void main(String[] args){
        TransportClient cli = EsClient.getCli();
        TermQueryBuilder query = QueryBuilders.termQuery("_id", "41dc3eead1d5ad1c0f6ef04b56394f0b");
        SearchResponse searchResponse = cli.prepareSearch("article_m").setTypes("wxArticle").setQuery(query).get();
        System.out.println(searchResponse.getHits().getTotalHits());
    }
}
