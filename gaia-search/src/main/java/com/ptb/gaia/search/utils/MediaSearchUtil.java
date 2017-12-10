package com.ptb.gaia.search.utils;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.transport.RemoteTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * Created by lishuangbin on 16/7/1.
 */
public class MediaSearchUtil {
    public static final Logger logger = LoggerFactory
            .getLogger(MediaSearchUtil.class);
    private InetSocketTransportAddress[] serverAddresses;
    private static MediaSearchUtil mediaSearchUtil;
    private static Client client;

    private PropertiesConfiguration conf;
    private EsData esData;

    public MediaSearchUtil(){
        try {
            conf = new PropertiesConfiguration("ptb.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        esData = new EsData();
        esData.setHostNames(conf.getStringArray("gaia.es.hosts"));
        esData.setClusterName(conf.getString("gaia.es.clusterName", "ptbes"));
        esData.setIndexName(conf.getString("gaia.es.media.index", "media_test"));
        esData.setTTl(conf.getLong("gaia.es.ttl", 432000));
        esData.setBatchSize(conf.getInt("gaia.es.batchSize", 2000));

        configureHostnames(esData.getHostNames());
        openClient(esData.getClusterName());
    }

    private void configureHostnames(String[] hostNames) {
        logger.warn(Arrays.toString(hostNames));
        serverAddresses = new InetSocketTransportAddress[hostNames.length];
        for (int i = 0; i < hostNames.length; i++) {
            String[] hostPort = hostNames[i].trim().split(":");
            String host = hostPort[0].trim();
            int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim()) : 9300;
            serverAddresses[i] = new InetSocketTransportAddress(new InetSocketAddress(host, port));
        }
    }

    private void openClient(String clusterName) {
        logger.info("Using ElasticSearch hostnames: {} ",
                Arrays.toString(serverAddresses));
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clusterName).build();

        TransportClient transportClient = TransportClient.builder().settings(settings).build();
        for (InetSocketTransportAddress host : serverAddresses) {
            transportClient.addTransportAddress(host);
        }
        if (client != null) client.close();
        client = transportClient;
    }

    public Client getClient() {
        return client;
    }

    public static MediaSearchUtil getInstanse(){
        if (null == mediaSearchUtil) {
            mediaSearchUtil = new MediaSearchUtil();
        }

        return mediaSearchUtil;
    }

    public SearchResponse query(QueryBuilder qb, SortBuilder sort, int startNum, int offset, String indexType){
        Client client = this.getClient();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(esData.getIndexName()).setTypes(indexType).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

        if (qb != null) {
            searchRequestBuilder.setQuery(qb);
        }

        if (sort != null){
            searchRequestBuilder.addSort(sort);
        }

        if (offset > 0){
            searchRequestBuilder.setFrom(startNum).setSize(offset);
        }

        SearchResponse searchResponse = searchRequestBuilder.setTrackScores(true).execute().actionGet();
        return searchResponse;
    }

    public SearchHits matchQueryMedia(QueryBuilder qb, SortBuilder sort, int startNum, int offset, String indexType){
        if(qb == null || offset <= 0){
            return null;
        }
        SearchResponse response;
        response = this.query(qb, sort, startNum, offset, indexType);
        logger.info("indexType: {}; searchArticle: {}; searchArticle millis: {}",indexType, qb.toString(), response.getTookInMillis());
        return response.getHits();
    }

    public static void print(SearchResponse response){
        System.out.println(response);
        response.getHits().forEach(e -> {
            System.out.println(e.sourceAsString());
            System.out.println(e.getScore());
        });
        return;
    }
}
