package com.ptb.gaia.index.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ptb.gaia.index.EsIndexAbstract;
import com.ptb.gaia.utils.EsClient;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.transport.TransportResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;

/**
 * Created by watson zhang on 2016/11/24.
 */
public class IndexServerEs {

    private static Logger logger = LoggerFactory.getLogger(IndexServerEs.class);

    static Configuration conf;
    public Client cli;
    public String esIndex;
    public String esWxType;
    public String esWbType;
    public int banchSize;
    private BulkRequestBuilder bulkRequestBuilder;
    private static long startTime;
    private static long timeMinute;
    private static long inputNum;
    private static long lastinputNum;
    BulkProcessor bulkBuilder;
    String ttlStr;
    private ArticleConvert articleConvert;
    private MediaConvert mediaConvert;

    public IndexServerEs(){
        try {
            conf = new PropertiesConfiguration("ptb.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        esIndex = conf.getString("gaia.search.media.esIndex", "media");
        esWxType = conf.getString("gaia.search.media.esWxType", "weixinMedia");
        esWbType = conf.getString("gaia.search.media.esWbType", "weiboMedia");
        banchSize = conf.getInteger("gaia.search.media.banchSize", 3000);
        ttlStr = conf.getString("gaia.search.media.ttl", "90d");
        cli = EsClient.getCli();
        bulkRequestBuilder = cli.prepareBulk();
        startTime = System.currentTimeMillis();
        articleConvert = new ArticleConvert();
        mediaConvert = new MediaConvert();

        bulkBuilder = BulkProcessor.builder(cli, new BulkProcessor.Listener() {
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
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))     //每15MB刷新一次
                //.setBulkActions(1500)                                         //每1500个request刷新一次
                .setFlushInterval(TimeValue.timeValueSeconds(3))        //每隔3秒刷新一次
                .setConcurrentRequests(5)                               //有5个线程处理请求
                .build();

        logger.info("indexName: {}\n" +
                        " wxType: {}\n" +
                        " wbType: {}\n" +
                        " batchSize: {}\n" +
                        " intervalTime: {}\n" +
                        " lastTime: {}",
                esIndex,
                esWxType,
                esWbType,
                banchSize);
    }

    public TransportResponse search(String indexName, String indexType, String searchType, String searchValue){
        QueryBuilder queryBuilder = matchPhraseQuery(searchType, searchValue);
        SearchResponse searchResponse = cli.prepareSearch(indexName)
                .setTypes(indexType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder)
                .setExplain(true)
                .execute()
                .actionGet();
        return searchResponse;
    }

    @Deprecated
    public void insert(String indexName, String indexType, String id, XContentBuilder msg){
        cli.prepareIndex(indexName, indexType, id).setSource(msg).execute().actionGet();
    }

    public void insert(String indexName, String indexType, String id, Map<String, Object> msg){
        cli.prepareIndex(indexName, indexType, id).setSource(msg).execute().actionGet();
    }

    public void upsertMedia(String indexName, String indexType, String idType, String idValue, Map<String, Object> msg){
        TransportResponse response = this.search(indexName, indexType, idType, idValue);

        double score = 0;
        JSONObject jsonObject = null;
        if (((SearchResponse) response).getHits().getTotalHits() <= 0){
            jsonObject = new JSONObject();
        }else {
            jsonObject = JSON.parseObject(JSON.toJSONString(((SearchResponse) response).getHits().getAt(0).getSource()));
        }

        if (indexName.equals(EsIndexAbstract.mediaIndex)){
            if (indexType.equals(EsIndexAbstract.wxMediaType)){
                score = mediaConvert.getMediaScore(1, jsonObject, msg);
            }else if (indexType.equals(EsIndexAbstract.wbMediaType)){
                score = mediaConvert.getMediaScore(2, jsonObject, msg);
            }
            msg.put("mediaScore", score);
        }
        this.upsertNotScore(indexName, indexType, idValue, msg);
    }

    public void upsertArticle(String indexName, String indexType, String idType, String idValue, Map<String, Object> msg){
        TransportResponse response = this.search(indexName, indexType, idType, idValue);

        double score = 0;
        JSONObject jsonObject = null;
        if (((SearchResponse) response).getHits().getTotalHits() <= 0){
            jsonObject = new JSONObject();
        }else {
            jsonObject = JSON.parseObject(JSON.toJSONString(((SearchResponse) response).getHits().getAt(0).getSource()));
        }

        if (indexType.equals(EsIndexAbstract.wxArticleType)){
            score = articleConvert.getArticleScore(1, jsonObject, msg);
        }else if (indexType.equals(EsIndexAbstract.wbArticleType)){
            score = articleConvert.getArticleScore(2, jsonObject, msg);
        }
        msg.put("relativity", score);
        this.upsertNotScore(indexName, indexType, idValue, msg);

    }

    public void upsertNotScore(String indexName, String indexType, String idValue, Map<String, Object> msg) {
        IndexRequestBuilder indexRequestBuilder = cli.prepareIndex(indexName, indexType, idValue);
        indexRequestBuilder.setSource(msg);
        IndexRequest indexRequest = indexRequestBuilder.request();

        UpdateRequestBuilder updateRequestBuilder = cli.prepareUpdate(indexName, indexType, idValue).setDoc(msg);
        if (indexName.equals(EsIndexAbstract.articleTitleIndex) || indexName.equals(EsIndexAbstract.articleContentIndex)){
            updateRequestBuilder.setTtl(ttlStr);
        }
        UpdateRequest updateRequest = updateRequestBuilder.setUpsert(indexRequest).request();

        bulkBuilder.add(updateRequest);

        this.logSpeed();
    }

    public void logSpeed(){
        inputNum ++;

        long endTime = System.currentTimeMillis();
        if ((endTime - startTime)/60000 > timeMinute){
            timeMinute++;
            logger.info("intputTime: {}, inputNum:{}, inputSpeed/min:{}",timeMinute, inputNum, inputNum - lastinputNum);
            lastinputNum = inputNum;
        }
    }

    public void addMsg(Map<String, Object> contMap, String index, String type, String id){
        IndexRequestBuilder indexRequestBuilder = cli.prepareIndex(index, type, id).setSource(contMap);
        if (index.equals(EsIndexAbstract.articleTitleIndex) || index.equals(EsIndexAbstract.articleContentIndex)){
            indexRequestBuilder.setTTL(ttlStr);
        }
        bulkBuilder.add(indexRequestBuilder.request());
    }

    public void flushEsBulk(){
        bulkBuilder.flush();
    }
}
