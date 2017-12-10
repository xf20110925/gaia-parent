package com.ptb.gaia.tool.esTool.util;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;

import com.ptb.gaia.tool.esTool.data.EsData;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.TransportResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;

/**
 * Created by watson zhang on 16/6/30.
 */
public class EsTransportClient {

    private static final Logger logger = LoggerFactory.getLogger(EsTransportClient.class);
    private int DEFAULT_PORT = 9300;
    private PropertiesConfiguration conf;

    private BulkRequestBuilder bulkRequestBuilder;
    private BulkProcessor bulkBuilder;
    private InetSocketTransportAddress[] serverAddresses;
    private Client client;
    private EsData esData;
    private String WEIBOTYPE = "weiboMedia";
    private String WEIXINTYPE = "weixinMedia";

    public EsTransportClient(){
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
        esData.setClusterName(conf.getString("gaia.es.clusterName", "ptbes"));
        esData.setTTl(conf.getLong("gaia.es.ttl", 432000));
        esData.setBatchSize(conf.getInt("gaia.es.batchSize", 3000));

        configureHostnames(esData.hostNames);
        openClient(esData.clusterName);
        bulkRequestBuilder = client.prepareBulk();

        bulkBuilder = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

            }
        })
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))     //每5MB刷新一次
                //.setBulkActions(1500)                                         //每1500个request刷新一次
                .setFlushInterval(TimeValue.timeValueSeconds(30))        //每隔30秒刷新一次
                .setConcurrentRequests(5)                               //有5个线程处理请求
                .build();
    }

    public EsData getEsData() {
        return esData;
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

    public void addMsg(XContentBuilder contMap, String index, String type, String id){
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, type, id).setSource(contMap);
        bulkBuilder.add(indexRequestBuilder.request());
    }

    public void addMsg(String source, String index, String type, String id){
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, type, id).setSource(source).setRouting("");
        bulkBuilder.add(indexRequestBuilder.request());
    }

    public void addMsg(Map source, String index, String type, String id){
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, type, id).setSource(source).setRouting("");
        bulkBuilder.add(indexRequestBuilder.request());
    }

    public void excute(){
        try {
            if (bulkRequestBuilder.numberOfActions() <= 0){
                return;
            }
            BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                //throw new EventDeliveryException(bulkResponse.buildFailureMessage());
            }
            bulkRequestBuilder = client.prepareBulk();
        }catch (Exception e){
            e.printStackTrace();
            bulkRequestBuilder = client.prepareBulk();
        } finally {
            bulkRequestBuilder = client.prepareBulk();
        }
    }

    public int getBulkNum(){
        if (bulkRequestBuilder != null){
            return bulkRequestBuilder.numberOfActions();
        }else {
            logger.error("bulkRequestBuilder is null");
            return -1;
        }
    }

    public TransportResponse search( String indexName, String indexType, String searchTyep, String searchValue){
        QueryBuilder queryBuilder = matchPhraseQuery(searchTyep, searchValue);
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setTypes(indexType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder)
                .setExplain(true)
                .execute()
                .actionGet();
        return searchResponse;
    }

    public void insert(String indexName, String indexType, String id, XContentBuilder msg){
        client.prepareIndex(indexName, indexType, id).setSource(msg).execute().actionGet();
    }

    public void insert(String indexName, String indexType, String id, Map msg){
        client.prepareIndex(indexName, indexType, id).setSource(msg).execute().actionGet();
    }

    public void upsert(String indexName, String indexType, String idType, String idValue, XContentBuilder msg) throws IOException {
        if (bulkRequestBuilder == null){
            bulkRequestBuilder = client.prepareBulk();
        }
        TransportResponse response = this.search(indexName, indexType, idType, idValue);

        if (((SearchResponse) response).getHits().getTotalHits() <= 0){
            String pmid;
            if((pmid = JSON.parseObject(msg.string()).get("pmid").toString()) == null){
                return ;
            }
            this.insert(indexName, indexType, pmid, msg);
        }else {
            XContentBuilder builder = null;
            try {
                builder = XContentFactory.jsonBuilder().startObject();
                ((SearchResponse) response).toXContent(builder, new ToXContent.MapParams(ImmutableMap.<String, String>of()));
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (SearchHit searchHit : ((SearchResponse) response).getHits()) {
                String pmidTmp = searchHit.getId();
                IndexRequest indexRequest = new IndexRequest(indexName, indexType, pmidTmp).source(builder);
                UpdateRequest updateRequest = new UpdateRequest(indexName, indexType, pmidTmp).doc(msg).upsert(indexRequest);
                UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(indexName, indexType, pmidTmp).setDoc(updateRequest.doc());
                bulkRequestBuilder.add(updateRequestBuilder);
            }
        }
    }

    public void upsert(String indexName, String indexType, String idType, String idValue, Map<String, Object> msg) throws IOException {
        TransportResponse response = this.search(indexName, indexType, idType, idValue);

        if (((SearchResponse) response).getHits().getTotalHits() <= 0){
            this.insert(indexName, indexType, idValue, msg);
        }else {
            IndexRequestBuilder indexRequestBuilder = client.prepareIndex(indexName, indexType, idValue);
            indexRequestBuilder.setSource(msg);
            IndexRequest indexRequest = indexRequestBuilder.request();

            UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(indexName, indexType, idValue);
            updateRequestBuilder.setDoc(msg);
            UpdateRequest updateRequest = updateRequestBuilder.request();
            bulkBuilder.add(updateRequest.upsert(indexRequest));
        }
    }

    public void weiboUpsert(String idType, String idValue, XContentBuilder msg){
        try {
            this.upsert(esData.getIndexName(), WEIBOTYPE, idType, idValue, msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void weixinUpsert(String idType, String idValue, XContentBuilder msg){
        try {
            this.upsert(esData.getIndexName(), WEIXINTYPE, idType, idValue, msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Client getClient() {
        return client;
    }
}
