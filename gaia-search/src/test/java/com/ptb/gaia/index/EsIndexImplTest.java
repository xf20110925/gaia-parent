package com.ptb.gaia.index;

import com.alibaba.fastjson.JSONObject;
import com.ptb.gaia.index.bean.EsInsertData;
import com.ptb.gaia.index.bean.WxReqMediaIndex;
import com.ptb.gaia.index.es.IndexServerEs;
import org.apache.commons.collections.map.HashedMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by watson zhang on 2016/11/25.
 */
public class EsIndexImplTest {

    EsArticleIndexImp esArticleIndexImp = new EsArticleIndexImp();
    EsMediaIndexImp esMediaIndexImp = new EsMediaIndexImp();
    //System.out.println(JSON.parseObject(builder.string()).get("pmid"));  //输出XContentBuilder 内容
    @Test
    public void insertWxMediaOne() throws Exception {
        JSONObject document = new JSONObject();
        document.put("pmid","value1");
        document.put("mediaName","value2");
        EsInsertData esInsertData = new EsInsertData();
        esInsertData.setId("weixinOne");
        esInsertData.setJsonObject(document);
        esMediaIndexImp.insertWxMediaOne(esInsertData);
    }

    @Test
    public void insertWbMediaOne() throws Exception {
        JSONObject document = new JSONObject();
        document.put("pmid","value1");
        document.put("mediaName","value2");
        EsInsertData esInsertData = new EsInsertData();
        esInsertData.setId("weiboOne");
        esInsertData.setJsonObject(document);
        esMediaIndexImp.insertWbMediaOne(esInsertData);
    }

    @Test
    public void insertWxMediaBatch() throws Exception {
        int i = 0;
        List<EsInsertData> esInsertDatas = new ArrayList<>();
        for (;i< 10;i++) {
            JSONObject document = new JSONObject();
            String value = String.format("value_%d", i);
            document.put("pmid", "value1");
            document.put("mediaName", "value2");
            String id = String.format("id_%d", i);
            EsInsertData esInsertData = new EsInsertData();
            esInsertData.setId(id);
            esInsertData.setJsonObject(document);
            esInsertDatas.add(esInsertData);
        }
        esMediaIndexImp.insertWxMediaBatch(esInsertDatas);
    }

    @Test
    public void insertWbMediaBatch() throws Exception {
        int i = 0;
        List<EsInsertData> esInsertDatas = new ArrayList<>();
        for (;i< 10;i++){
            JSONObject document = new JSONObject();
            String value = String.format("value_%d", i);
            document.put("pmid","value1");
            document.put("mediaName","value2");
            document.put("key", value);
            String id = String.format("id_%d", i);
            EsInsertData esInsertData = new EsInsertData();
            esInsertData.setId(id);
            esInsertData.setJsonObject(document);
            esInsertDatas.add(esInsertData);
        }
        esMediaIndexImp.insertWbMediaBatch(esInsertDatas);
    }

    @Test
    public void excuteInsert() throws Exception {

    }

    @Test
    public void getBulkNum() throws Exception {

    }

    @Test
    public void upsertWxMediaOne() throws Exception {

    }

    @Test
    public void upsertWbMediaOne() throws Exception {

    }

    @Test
    public void upsertWxMediaBatch() throws Exception {
        int i = 0;
        List<EsInsertData> esInsertDatas = new ArrayList<>();
        for (;i< 10;i++){
            JSONObject doc = new JSONObject();
            //doc.put("mediaName", "aaaggassdf");
            doc.put("biz", "basdgafdazcxo");
            //doc.put("weixinId", 10000);
            String id = String.format("wxbatchId_%d", i);
            EsInsertData esInsertData = new EsInsertData();
            esInsertData.setId(id);
            esInsertData.setJsonObject(doc);
            esInsertDatas.add(esInsertData);
        }
        esMediaIndexImp.upsertWxMediaBatch(esInsertDatas);
    }

    @Test
    public void upsertWbMediaBatch() throws Exception {
        int i = 0;
        List<EsInsertData> esInsertDatas = new ArrayList<>();
        for (;i< 10;i++){
            JSONObject doc = new JSONObject();
            //doc.put("mediaName", "aaaggassdf");
            //doc.put("weiboId", "basdg");
            doc.put("fans", 10000);
            //doc.put("")
            String id = String.format("wxbatchId_aaaa%d", i);
            EsInsertData esInsertData = new EsInsertData();
            esInsertData.setId(id);
            esInsertData.setJsonObject(doc);
            esInsertDatas.add(esInsertData);
        }
        esMediaIndexImp.upsertWbMediaBatch(esInsertDatas);
    }

    @Test
    public void insertWxArticleTitleOne() throws Exception {
        JSONObject doc = new JSONObject();
        doc.put("title", "汽车术语大全！！！");
        doc.put("biz", "bbbbbbbbb");
        doc.put("reads", "100");
        doc.put("original", "0");
        doc.put("postTime", "1471260868000");
        doc.put("relativity", "0.7");
        doc.put("url", "http://www.baidu.com");

        EsInsertData esInsertData = new EsInsertData();
        esInsertData.setJsonObject(doc);
        esInsertData.setId("wxArticleTitleOne2");
        esArticleIndexImp.insertWxArticleTitleOne(esInsertData);
    }

    @Test
    public void insertWbArticleTitleOne() throws Exception {
        JSONObject doc = new JSONObject();
        doc.put("title", "aaaggassdf");
        doc.put("pmid", "basdglajslgj");
        EsInsertData esInsertData = new EsInsertData();
        esInsertData.setId("wbArticleTitleOne");
        esInsertData.setJsonObject(doc);
        esArticleIndexImp.insertWbArticleTitleOne(esInsertData);
    }

    @Test
    public void insertWxArticleTitleBatch() throws Exception {
        int i = 0;
        List<EsInsertData> esInsertDatas = new ArrayList<>();
        for (;i< 10;i++){
            JSONObject doc = new JSONObject();
            doc.put("title", "aaaggassdf");
            doc.put("pmid", "basdglajslgj");
            String id = String.format("wxbatchId_%d", i);
            EsInsertData esInsertData = new EsInsertData();
            esInsertData.setId(id);
            esInsertData.setJsonObject(doc);
            esInsertDatas.add(esInsertData);
        }
        esArticleIndexImp.insertWxArticleTitleBatch(esInsertDatas);
    }

    @Test
    public void insertWbArticleTitleBatch() throws Exception {
        int i = 0;
        List<EsInsertData> esInsertDatas = new ArrayList<>();
        for (;i< 10;i++){
            JSONObject doc = new JSONObject();
            doc.put("title", "aaaggassdf");
            doc.put("pmid", "basdglajslgj");
            String id = String.format("wbbatchId_%d", i);
            EsInsertData esInsertData = new EsInsertData();
            esInsertData.setId(id);
            esInsertData.setJsonObject(doc);
            esInsertDatas.add(esInsertData);
        }
        esArticleIndexImp.insertWbArticleTitleBatch(esInsertDatas);

    }

    @Test
    public void insertWxArticleContentOne() throws Exception {
        JSONObject doc = new JSONObject();
        doc.put("content", "中国");
        doc.put("biz", "MjM5MzAzNzE0MQ==");
        doc.put("likes", "100");
        doc.put("original", "0");
        doc.put("postTime", "100000000");
        doc.put("url", "http://www.baidu.com");
        EsInsertData esInsertData = new EsInsertData();
        esInsertData.setId("wxArticleContentOne");
        esInsertData.setJsonObject(doc);
        esArticleIndexImp.insertWxArticleContentOne(esInsertData);
    }

    @Test
    public void insertWbArticleContentOne() throws Exception {
        JSONObject doc = new JSONObject();
        doc.put("content", "aaaggassdf");
        doc.put("pmid", "basdglajslgj");
        EsInsertData esInsertData = new EsInsertData();
        esInsertData.setId("wbArticleContentOne");
        esInsertData.setJsonObject(doc);
        esArticleIndexImp.insertWbArticleContentOne(esInsertData);
    }

    @Test
    public void insertWxArticleContentBatch() throws Exception {
        int i = 0;
        List<EsInsertData> esInsertDatas = new ArrayList<>();
        for (;i< 10;i++){
            JSONObject doc = new JSONObject();
            doc.put("content", "aaaggassdf");
            doc.put("pmid", "basdglajslgj");
            String id = String.format("wxArticleContentBatch_%d", i);
            EsInsertData esInsertData = new EsInsertData();
            esInsertData.setId(id);
            esInsertData.setJsonObject(doc);
            esInsertDatas.add(esInsertData);
        }
        esArticleIndexImp.insertWxArticleContentBatch(esInsertDatas);
    }

    @Test
    public void insertWbArticleContentBatch() throws Exception {
        int i = 0;
        List<EsInsertData> esInsertDatas = new ArrayList<>();
        for (;i< 10;i++){
            JSONObject doc = new JSONObject();
            doc.put("content", "aaaggassdf");
            doc.put("pmid", "basdglajslgj");
            String id = String.format("wbArticleContentBatch_%d", i);
            EsInsertData esInsertData = new EsInsertData();
            esInsertData.setId(id);
            esInsertData.setJsonObject(doc);
            esInsertDatas.add(esInsertData);
        }
        esArticleIndexImp.insertWbArticleContentBatch(esInsertDatas);
    }

    @Test
    public void upsertWxArticleTitleBatch() throws Exception {
        int i = 0;
        List<EsInsertData> esInsertDatas = new ArrayList<>();
        for (;i< 10;i++){
            JSONObject doc = new JSONObject();
            doc.put("title", "aaaggas");
            doc.put("pmid", "basdglajslgj");
            String id = String.format("wxATitleBatch_%d", i);
            EsInsertData esInsertData = new EsInsertData();
            esInsertData.setId(id);
            esInsertData.setJsonObject(doc);
            esInsertDatas.add(esInsertData);
        }
        esArticleIndexImp.upsertWxArticleTitleBatch(esInsertDatas);
    }

    @Test
    public void upsertWxArticleContentBatch() throws Exception {
        int i = 0;
        List<EsInsertData> esInsertDatas = new ArrayList<>();
        for (;i< 10;i++){
            JSONObject doc = new JSONObject();
            doc.put("content", "aaagasdfasgasgassdf");
            doc.put("pmid", "basdglajslgj");
            String id = String.format("wxAContentBatch_%d", i);
            EsInsertData esInsertData = new EsInsertData();
            esInsertData.setId(id);
            esInsertData.setJsonObject(doc);
            esInsertDatas.add(esInsertData);
        }
        esArticleIndexImp.upsertWxArticleContentBatch(esInsertDatas);
    }

    @Test
    public void upsertWbArticleTitleBatch() throws Exception {
        int i = 0;
        List<EsInsertData> esInsertDatas = new ArrayList<>();
        for (;i< 10;i++){
            JSONObject doc = new JSONObject();
            doc.put("title", "aaaggassdf wb article title");
            doc.put("original", true);
            doc.put("pmid", "basdglajslgj");
            String id = String.format("wbATitleBatch_%d", i);
            EsInsertData esInsertData = new EsInsertData();
            esInsertData.setId(id);
            esInsertData.setJsonObject(doc);
            esInsertDatas.add(esInsertData);
        }
        esArticleIndexImp.upsertWbArticleTitleBatch(esInsertDatas);
    }

    @Test
    public void upsertWbArticleContentBatch() throws Exception {
        int i = 0;
        List<EsInsertData> esInsertDatas = new ArrayList<>();
        for (;i< 10;i++){
            JSONObject doc = new JSONObject();
            doc.put("content", "aaaggassdf wb article content");
            doc.put("pmid", "basdglajslgj");
            String id = String.format("wbAContentBatch_%d", i);
            EsInsertData esInsertData = new EsInsertData();
            esInsertData.setId(id);
            esInsertData.setJsonObject(doc);
            esInsertDatas.add(esInsertData);
        }
        esArticleIndexImp.upsertWbArticleContentBatch(esInsertDatas);
    }

    @Test
    public void upsertLiveMediaFromMongo(){
        EsInsertData esInsertData = new EsInsertData();
        JSONObject doc = new JSONObject();
        doc.put("content", "aaaggassdf wb article content");
        doc.put("pmid", "basdglajslgj");
        doc.put("mediaName", "basdglajslgj");
        String id = String.format("wbAContentBatch_%d", 1);
        esInsertData.setId(id);
        esInsertData.setJsonObject(doc);
        esMediaIndexImp.insertLiveMediaFromMongo(esInsertData);
        esMediaIndexImp.flushEsBulk();
        return;
    }

    @Test
    public void upsertBindMediaTest(){
        WxReqMediaIndex wxMediaReq = new WxReqMediaIndex();
        List<Integer> iIds = new ArrayList<>();
        iIds.add(10000);
        iIds.add(120130);
        wxMediaReq.setiIds(iIds);
        wxMediaReq.setPlatType(1);
        wxMediaReq.setMediaType(1);
        wxMediaReq.setHlavgRead(100);
        wxMediaReq.setHlavgZan(20);
        wxMediaReq.setIsOriginal(1);
        wxMediaReq.setAgentNum(5);
        wxMediaReq.setIsAuth(1);
        wxMediaReq.setMediaName("同道大叔");
        wxMediaReq.setPmid("aaaaaaaaaaac");
        wxMediaReq.setPrice(100000);


        esMediaIndexImp.insertWebSearchMedia(wxMediaReq);
        esMediaIndexImp.flushEsBulk();
    }


    @Test
    public void upsetNotScoreTest(){
        IndexServerEs indexServerEs = new IndexServerEs();
        Map<String, Object> esMap = new HashedMap();
        //esMap.put("pmid", "aaaaaa");
        esMap.put("mediaName", "bbbaa");
        //esMap.put("auth", "cccc");
        indexServerEs.upsertNotScore("media_test_c", "wbMedia", "wbTest_a", esMap);
        indexServerEs.flushEsBulk();
    }
}
