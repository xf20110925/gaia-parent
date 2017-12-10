package com.ptb.gaia.tool.esTool.search;

import com.alibaba.fastjson.JSON;
import com.ptb.gaia.service.IGaia;
import com.ptb.gaia.service.IGaiaImpl;
import com.ptb.gaia.service.entity.media.GWbMedia;
import com.ptb.gaia.service.entity.media.GWxMedia;
import com.ptb.gaia.tool.esTool.data.EsData;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.TransportResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by watson zhang on 2016/12/23.
 */
public class SearchArticle {
    private static Logger logger = LoggerFactory.getLogger(SearchArticle.class);

    private int DEFAULT_PORT = 9300;
    private Client client;
    private EsData esData;
    private PropertiesConfiguration conf;
    private InetSocketTransportAddress[] serverAddresses;
    private String searchScript;
    private static IGaia iGaia;

    public SearchArticle(){
        esData = new EsData();
        if(esData == null){
            logger.error("es data is null");
            return;
        }
        try {
            conf = new PropertiesConfiguration("ptb.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        esData.setHostNames(conf.getStringArray("gaia.es.hosts"));
        esData.setClusterName(conf.getString("gaia.es.clusterName", "test-es"));
        esData.setTTl(conf.getLong("gaia.es.ttl", 432000));
        esData.setBatchSize(conf.getInt("gaia.es.batchSize", 1200));
        esData.setIndexName(conf.getString("gaia.es.article.index", "media"));
        searchScript = conf.getString("gaia.es.script", "return _score");
        esData.setIndexType("wxArticle");
        try {
            iGaia = new IGaiaImpl();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        configureHostnames(esData.hostNames);
        openClient(esData.clusterName);
    }

    private void configureHostnames(String[] hostNames) {
        logger.warn(Arrays.toString(hostNames));
        serverAddresses = new InetSocketTransportAddress[hostNames.length];
        for (int i = 0; i < hostNames.length; i++) {
            String[] hostPort = hostNames[i].trim().split(":");
            String host = hostPort[0].trim();
            int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim())
                    : DEFAULT_PORT;
            serverAddresses[i] = new InetSocketTransportAddress(new InetSocketAddress(host, port));
        }
    }

    private void openClient(String clusterName) {
        logger.info("Using ElasticSearch hostnames: {} ",
                Arrays.toString(serverAddresses));
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clusterName)
                .build();

        TransportClient transportClient = TransportClient.builder().settings(settings).build();
        for (InetSocketTransportAddress host : serverAddresses) {
            transportClient.addTransportAddress(host);
        }
        if (client != null) {
            client.close();
        }
        client = transportClient;
    }

    private SearchResponse termSearchOne(String keyword, int start, int size){

        //String formula = "return log(doc['readNum'].value.toFloat()+1.0)*_score";
        Script script = new Script(searchScript);
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("title", keyword);
        SortBuilder sortBuilder = SortBuilders.scriptSort(script, "number").order(SortOrder.DESC);
        SearchResponse searchResponse = searchArticle(queryBuilder, sortBuilder, start, size);
        return searchResponse;
    }

    public static void print(SearchResponse response){
        if (response == null){
            return;
        }
        logger.warn("start_response");
        logger.warn("{}", response);
        logger.warn("end_response");

        logger.warn("start_detail_response");
        response.getHits().forEach(e -> {
            if (e.getType().equals("wxArticle")){
                GWxMedia pmid = iGaia.getWxMedia(e.getSource().get("pmid").toString());
                if (pmid != null){
                    e.getSource().put("mediaName", pmid.getMediaName());
                    if (pmid.getPi() != null){
                        e.getSource().put("pi", pmid.getPi());
                    }
                }
            }else if (e.getType().equals("wbArticle")){
                GWbMedia pmid = iGaia.getWbMedia(e.getSource().get("pmid").toString());
                if (pmid != null){
                    e.getSource().put("mediaName", pmid.getMediaName());
                }
            }else {
                System.out.println("type is error!");
            }
            e.getSource().put("title", resolveToByteFromEmoji(e.getSource().get("title").toString()));
            String jsonString = JSON.toJSONString(e.getSource());
            logger.warn("{}", jsonString);
        });
        logger.warn("end_detail_response");
        return;
    }

    public static String resolveToByteFromEmoji(String str) {
        Pattern pattern = Pattern
                .compile("[^(\u2E80-\u9FFF\\w\\s`~!@#\\$%\\^&\\*\\(\\)_+-？（）——=\\[\\]{}\\|;。，、《》”：；“！……’:'\"<,>\\.?/\\\\*)]");
        Matcher matcher = pattern.matcher(str);
        StringBuffer sb2 = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(sb2, resolveToByte(matcher.group(0)));
        }
        matcher.appendTail(sb2);
        return sb2.toString();
    }

    private static String resolveToByte(String str) {
        byte[] b = str.getBytes();
        StringBuffer sb = new StringBuffer();
        sb.append("<:");
        for (int i = 0; i < b.length; i++) {
            if (i < b.length - 1) {
                sb.append(Byte.valueOf(b[i]).toString() + ",");
            } else {
                sb.append(Byte.valueOf(b[i]).toString());
            }
        }
        sb.append(":>");
        return sb.toString();
    }

    private static String resolveToEmoji(String str) {
        str = str.replaceAll("<:", "").replaceAll(":>", "");
        String[] s = str.split(",");
        byte[] b = new byte[s.length];
        for (int i = 0; i < s.length; i++) {
            b[i] = Byte.valueOf(s[i]);
        }
        return new String(b);
    }

    public SearchResponse searchArticle(QueryBuilder queryBuilder, SortBuilder sortBuilder, int start, int size){



        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(esData.getIndexName())
                .setTypes(esData.getIndexType())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setFrom(start)
                .setSize(size)
                .setTrackScores(true);

        if (queryBuilder != null){
            searchRequestBuilder.setQuery(queryBuilder);
        }else {
            logger.error("query is null!");
            return null;
        }

        if (sortBuilder != null){
            searchRequestBuilder.addSort(sortBuilder);
        }

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        return searchResponse;
    }


    public static void searchArticleTest(String keyword, int start, int size){
        SearchArticle searchArticle = new SearchArticle();
        String keywordLocal = "壁纸";
        SearchResponse searchResponse = searchArticle.termSearchOne(keyword, start, size);
        SearchArticle.print(searchResponse);
    }

    public static void searchArticleExact(String keyword, int start, int size){
        SearchArticle searchArticle = new SearchArticle();
        String keywordLocal = "壁纸";
        MatchQueryBuilder titleQuery = QueryBuilders.matchPhraseQuery("title", keyword);
        SearchResponse searchResponse = searchArticle.searchArticle(titleQuery, null, start, size);
        SearchArticle.print(searchResponse);
    }
}
