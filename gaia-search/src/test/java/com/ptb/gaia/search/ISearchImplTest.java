package com.ptb.gaia.search;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.ptb.gaia.index.bean.LiveReqMediaSearch;
import com.ptb.gaia.index.bean.WbReqMediaSearch;
import com.ptb.gaia.index.bean.WxReqMediaSearch;
import com.ptb.gaia.search.article.ArticleSearchEsImpl;
import com.ptb.gaia.search.bean.PostTime;
import com.ptb.gaia.search.bean.ReadNum;
import com.ptb.gaia.search.bean.ReqArticleBase;
import com.ptb.gaia.search.media.LiveMediaSearch;
import com.ptb.gaia.search.media.WeiboMediaSearch;
import com.ptb.gaia.search.media.WeixinMediaSearch;
import com.ptb.gaia.search.utils.EsRspResult;
import com.ptb.gaia.search.utils.MongoUtil;
import com.ptb.gaia.service.entity.article.GArticleBasic;
import com.ptb.gaia.service.entity.article.GWbArticle;
import com.ptb.gaia.service.entity.article.GWxArticle;
import com.ptb.gaia.service.entity.media.GWxMedia;

import junit.framework.TestCase;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by eric on 16/8/10.
 */
public class ISearchImplTest {
    ISearch iSearch = new ISearchImpl();

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void searchMediaInfo() throws Exception {
        TestCase.assertTrue(iSearch.searchMediaInfo("www", 0, 10, GWxMedia.class).size() > 0);
    }

    @Test
    public void searchWxMediaInfo() throws Exception{
        String keyword = "微信";
        EsRspResult<GWxMedia> gWxMediaEsRspResult = iSearch.searchWxMedia(keyword, 0, 10);
        System.out.println(gWxMediaEsRspResult.getTotalNum());
    }

    @Test
    public void searchWbMediaInfo() throws Exception{
        String keyword = "微博";
        TestCase.assertTrue(iSearch.searchWbMedia(keyword, 0, 10).getMediaList().size() > 0);
    }

    /**
     * @Description: Test searchArticleContentByUrl
     * @author: mengchen
     * @Date: 2017/2/14
     */
    @Test
    public void searchArticleContentByUrl() throws Exception {
        Set<String> set = new HashSet<>();
        set.add("http://mp.weixin.qq.com/s?__biz=MzAwMDM4OTMxOA==&mid=2658084095&idx=1&sn=bf37c8c4b04783df4bba473a41460200#rd");
        TestCase.assertTrue(iSearch.searchArticleContentByUrl(set, GWxArticle.class).size() > 0);
        set.clear();
        set.add("http://m.weibo.cn/5066044867/EnnTzrA3F");
        TestCase.assertTrue(iSearch.searchArticleContentByUrl(set, GWbArticle.class).size() > 0);
    }

    @Test
    public void search() throws Exception {
        TestCase.assertTrue(iSearch.searchArticle("中国", 0, GWbArticle.class, 0, 10).size() == 10);
    }

    @Test
    public void search1() throws Exception {
        TestCase.assertTrue(iSearch.searchArticle("微信", 0, GWxArticle.class, 0, 10).size() == 10);
    }

    @Test
    public void searchType() throws Exception {
        TestCase.assertTrue(iSearch.searchArticle("销量", 0, GWxArticle.class, 0, 10).size() == 10);
    }

    @Test
    public void searchArticle() {
        ReqArticleBase reqArticleBase = new ReqArticleBase();
        reqArticleBase.setKeyword("设计");
        reqArticleBase.setMediaType(2);
        reqArticleBase.setSearchType(1);
        reqArticleBase.setSortType(8);
        reqArticleBase.setStart(0);
        reqArticleBase.setEnd(10);
        ReadNum readNum = new ReadNum();
        readNum.setStart(0);
        readNum.setEnd(0);
        PostTime postTime = new PostTime();
        postTime.setStart(0);
        postTime.setEnd(0);

        Pair<Long, List<GArticleBasic>> longListPair = iSearch.searchArticle(reqArticleBase, readNum, postTime);

        System.out.println(longListPair.getLeft());

    }

    @Test
    public void getArticleByPmid() throws Exception {
        TestCase.assertTrue(iSearch.contentSearchByPmid("MzA3OTM5NTkxNA==", GWxArticle.class, 0, 10).size() == 10);
    }

    @Test
    public void searchArticleContent() {
        Pair<Long, List<GWxArticle>> result = iSearch.searchArticleContent("中国", 1, 0, GWxArticle.class, 0, 10);
        System.out.println(result);
    }

    @Test
    public void searchMediaTest() throws ExecutionException, InterruptedException {
        List<String> pmidList = new ArrayList<>();
        MongoCollection<Document> mongoCollection;
        MongoUtil mongoUtil = new MongoUtil();
        mongoCollection = mongoUtil.mongoDatabase.getCollection("wbMedia");
        FindIterable<Document> documents = mongoCollection.find();
        documents = documents.skip(RandomUtils.nextInt(3000, 4000));
        int pmidNum = 0;
        for (Document document : documents) {
            pmidList.add(document.getString("_id"));
            pmidNum++;
            if (pmidNum >= 1000) {
                break;
            }
        }
        WeiboMediaSearch weiboMediaSearch = new WeiboMediaSearch();
        //
        //FutureTask<WeiboMediaSearch> futureTask = new FutureTask<WeiboMediaSearch>(new Callable<WeiboMediaSearch>() {
        //	@Override
        //	public WeiboMediaSearch call() throws Exception {
        //		WeiboMediaSearch weiboMediaSearch = new WeiboMediaSearch();
        //		weiboMediaSearch.searchWbMedia(pmidList, 0, 2000);
        //		return null;
        //	}
        //});
        //ExecutorService executor = Executors.newFixedThreadPool(100); //使用线程池
        //executor.submit(futureTask);
        //
        //executor.execute(futureTask);
        //
        //WeiboMediaSearch weiboMediaSearch = futureTask.get();
        //Thread.sleep(20000);
    }

    @Test
    public void searchMediaTotalTest() {
        String mediaName = "做一个漂亮女人";
        WeixinMediaSearch weixinMediaSearch = new WeixinMediaSearch();
        //EsRspResult<GWxMedia> gWxMediaEsRspResult = weixinMediaSearch.searchWeixinMedia(1, mediaName, 0, 10);
        EsRspResult<GWxMedia> gWxMediaEsRspResult = weixinMediaSearch.searchWeixinMedia(mediaName, 0, 10);
        System.out.println(gWxMediaEsRspResult.getTotalNum());
    }

    @Test
    public void searchArticleContentBuUrl(){
        Set<String> idList = new HashSet<>();
        idList.add("http://mp.weixin.qq.com/s?__biz=MzAwODcxODQ0MQ==&mid=2651230462&idx=2&sn=83f5084a1cc99bae10742861b2f546eb#rd");
        idList.add("http://mp.weixin.qq.com/s?__biz=MzA4NzQ0MzQzMg==&mid=2650922567&idx=2&sn=2aacd32b28b98f6b80c23a35dc7191ab#rd");
        idList.add("http://mp.weixin.qq.com/s?__biz=MzIyNTY5NDU2Ng==&mid=2247483752&idx=1&sn=945f8c7bc83ea461e4556f4d66fd520#rd");
        idList.add("http://mp.weixin.qq.com/s?__biz=MzAxODA3MTg4MA==&mid=2650817016&idx=4&sn=f979bff0e4fbfcbe624e5ff6cea147c#rd");
        ArticleSearchEsImpl articleSearchEs = new ArticleSearchEsImpl();
        List<String> strings = articleSearchEs.searchArticleContentByUrl(idList, GWxArticle.class);
        System.out.println(strings);
    }

    @Test
    public void searchLiveMediaTest(){

        LiveMediaSearch liveMediaSearch = new LiveMediaSearch();
        String mediaName = "宝宝";
        EsRspResult<String> esRspResult = liveMediaSearch.searchLiveInfo(mediaName, 0, 10);
        System.out.println(esRspResult.getTotalNum());
    }

    @Test
    public void searchLiveMediaForWebTest(){
        LiveMediaSearch liveMediaSearch = new LiveMediaSearch();

        LiveReqMediaSearch liveReqMediaSearch = new LiveReqMediaSearch();
        liveReqMediaSearch.setKey("");
        liveReqMediaSearch.setStart(0);
        liveReqMediaSearch.setEnd(10);
        liveReqMediaSearch.setLocation("北京");
        //liveReqMediaSearch.setIsAuth(1);
        //liveReqMediaSearch.setMinPrice(1);
        //liveReqMediaSearch.setMaxPrice(100000);

        EsRspResult<String> stringEsRspResult = liveMediaSearch.searchLiveInfoForWeb(liveReqMediaSearch);
        System.out.println(stringEsRspResult.getTotalNum());
    }

    @Test
    public void searchWeixinMediaForWebTest(){
        WeixinMediaSearch weixinMediaSearch = new WeixinMediaSearch();

        WxReqMediaSearch wxReqMediaSearch = new WxReqMediaSearch();
        wxReqMediaSearch.setKey("当时我就震惊了");

        wxReqMediaSearch.setStart(0);
        wxReqMediaSearch.setEnd(10);
        wxReqMediaSearch.setIsAuth(1);
        wxReqMediaSearch.setSort(1);
        wxReqMediaSearch.setSeller(1);
        wxReqMediaSearch.setMinPrice(100000);
        wxReqMediaSearch.setMaxPrice(1000000);
        wxReqMediaSearch.setClassify(1005000000);

        EsRspResult<String> stringEsRspResult = weixinMediaSearch.searchWxInfoForWeb(wxReqMediaSearch);
        System.out.println(stringEsRspResult.getTotalNum());


    }

    @Test
    public void searchWeiboMediaForWebTest(){
        WeiboMediaSearch weiboMediaSearch = new WeiboMediaSearch();

        WbReqMediaSearch wbReqMediaSearch = new WbReqMediaSearch();
        wbReqMediaSearch.setKey("DC大叔");
        wbReqMediaSearch.setStart(0);
        wbReqMediaSearch.setEnd(10);
        //wbReqMediaSearch.setMinForward(100);
        //wbReqMediaSearch.setMaxForward(1000);
        wbReqMediaSearch.setMinLike(50);
        wbReqMediaSearch.setMaxLike(100);
        //wbReqMediaSearch.setSeller(1);
        //liveReqMediaSearch.setIsAuth(1);
        //wxReqMediaSearch.setMinPrice(1);
        //wxReqMediaSearch.setMaxPrice(100000);

        //wbReqMediaSearch.setMinFans(100000);
        //wbReqMediaSearch.setMaxFans(1000000);

        //wbReqMediaSearch.setMinComment(10);
        //wbReqMediaSearch.setMaxComment(1000);
        //wbReqMediaSearch.setMinForward(10);
        //wbReqMediaSearch.setMaxForward(200);

        EsRspResult<String> stringEsRspResult = weiboMediaSearch.searchWbInfoRorWeb(wbReqMediaSearch);
        System.out.println(stringEsRspResult.getTotalNum());
    }

    @Test
    public void searchWeixinMediaTest(){
        WeixinMediaSearch weixinMediaSearch = new WeixinMediaSearch();
        EsRspResult<GWxMedia> gWxMediaEsRspResult = weixinMediaSearch.searchWeixinMediaTest("10000", 0, 10);

        System.out.println(gWxMediaEsRspResult.getTotalNum());

    }
}