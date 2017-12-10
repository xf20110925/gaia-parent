package gaia.service;

import com.ptb.gaia.service.IGaia;
import com.ptb.gaia.service.IGaiaImpl;
import com.ptb.gaia.service.entity.article.GArticleSet;
import com.ptb.gaia.service.entity.article.GWbArticle;
import com.ptb.gaia.service.entity.article.GWxArticle;
import com.ptb.gaia.service.entity.article.GWxArticleDynamic;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertTrue;

/**
 * Created by eric on 16/6/30.
 */
public class IGaiaArticleTest {
    IGaia iGaia;

    public IGaiaArticleTest() throws ConfigurationException {
        iGaia = new IGaiaImpl();
    }

    @Test
    public void getWxDynamics() throws Exception {
        String url = "http://mp.weixin.qq.com/s?__biz=MzA5NjM3OTMzNw==&mid=2657163712&idx=1&sn=1707eb5bc6745e00ba6b6c3a916f4a4d#rd";
        List<GWxArticleDynamic> wxDynamics = iGaia.getWxDynamics(url, 0L, System.currentTimeMillis());
        assertTrue(wxDynamics.size() > 0);

        wxDynamics = iGaia.getWxDynamics(url, 0L, 0L);
        assertTrue(wxDynamics.size() == 0);
    }


    @Test
    public void getWxArticle() throws Exception {
        String url = "http://mp.weixin.qq.com/s?__biz=MzI3NzI5ODg2OQ==&mid=2247483715&idx=1&sn=46c9302e1004c10b90b81324a56cac89#rd";
        List<GWxArticle> wxArtcile = iGaia.getWxArtcile(Arrays.asList(url));
        assertTrue(wxArtcile.size() > 0);

    }

    List<String> weixinDefaultMedia = Arrays.asList("MzAwMDAyMzY3OA==", "MzA4MDE4OTYxNA==", "MzA4MzAxMjExNw==","MjM5MjEzNzYzOA==","MjM5NDMxMjM5Nw==");
    List<String> weiboDefaultMedia = Arrays.asList("1670183810", "3205344007", "3147304154");

    @Test
    public void getWxHotArticles() throws Exception {

        /*测试返回的数量 start 0 end 0*/
        GArticleSet<GWxArticle> wxHotArticles = iGaia.getWxHotArticles(weixinDefaultMedia, 0, 0);
        assertTrue(wxHotArticles.getArticles().size() == 0);
        assertTrue(wxHotArticles.getTotal() > 0);

        /*测试返回的数量 start 0 end -1*/
        wxHotArticles = iGaia.getWxHotArticles(weixinDefaultMedia, 0, -1);
        assertTrue(wxHotArticles.getArticles().size() == 0);
        assertTrue(wxHotArticles.getTotal() > 0);

        /*测试返回的数量 start 0 end 3*/
        wxHotArticles = iGaia.getWxHotArticles(weixinDefaultMedia, 0, 3);
        assertTrue(wxHotArticles.getArticles().size() == 3);
        assertTrue(wxHotArticles.getTotal() > 0);

        /*测试媒体返回数量*/
        int count1 = iGaia.getWxRecentArticleCount(weixinDefaultMedia);
        assertTrue(count1 > 0);


        /*测试返回的数量 pmid为空的情况*/
        wxHotArticles = iGaia.getWxHotArticles(null, 0, 3);
        assertTrue(wxHotArticles.getArticles().size() == 0);
        assertTrue(wxHotArticles.getTotal() == 0);


        /*测试返回的数量 pmid为没查到的情况的情况*/
        wxHotArticles = iGaia.getWxHotArticles(Arrays.asList("fsdafsdf"), 0, 3);
        assertTrue(wxHotArticles.getArticles().size() == 0);
        assertTrue(wxHotArticles.getTotal() == 0);


        /*测试返回的数量 start 2 end 4*/
        wxHotArticles = iGaia.getWxHotArticles(weixinDefaultMedia, 2, 4);
        assertTrue(wxHotArticles.getArticles().size() == 2);
        assertTrue(wxHotArticles.getTotal() > 0);

        wxHotArticles = iGaia.getWxHotArticles(weixinDefaultMedia, 0, 30);
        wxHotArticles.getArticles().stream().reduce((k1, k2) -> {
            int i = k1.getReadNum().compareTo(k2.getReadNum());
            assertTrue(k1.getReadNum() >= k2.getReadNum());
            if (i == 0) {
                assertTrue(k1.getLikeNum() >= k2.getLikeNum());
            }
            return k2;
        });
        assertTrue(wxHotArticles.getTotal() > 0);
    }

    @Test
    public void getWbHotArticles() throws Exception {

        /*测试返回的数量 start 0 end 0*/
        GArticleSet<GWbArticle> wbHotArticles = iGaia.getWbHotArticles(weiboDefaultMedia, 0, 0);
        assertTrue(wbHotArticles.getArticles().size() == 0);
        assertTrue(wbHotArticles.getTotal() > 0);

        /*测试返回的数量 start 0 end -1*/
        wbHotArticles = iGaia.getWbHotArticles(weiboDefaultMedia, 0, -1);
        assertTrue(wbHotArticles.getArticles().size() == 0);
        assertTrue(wbHotArticles.getTotal() > 0);

        /*测试返回的数量 start 0 end 3*/
        wbHotArticles = iGaia.getWbHotArticles(weiboDefaultMedia, 0, 3);
        assertTrue(wbHotArticles.getArticles().size() == 3);
        assertTrue(wbHotArticles.getTotal() > 0);

        /*测试返回的数量 pmid为空的情况*/
        wbHotArticles = iGaia.getWbHotArticles(null, 0, 3);
        assertTrue(wbHotArticles.getArticles().size() == 0);
        assertTrue(wbHotArticles.getTotal() == 0);


        /*测试返回的数量 pmid为没查到的情况的情况*/
        wbHotArticles = iGaia.getWbHotArticles(Arrays.asList("fsdafsdf"), 0, 3);
        assertTrue(wbHotArticles.getArticles().size() == 0);
        assertTrue(wbHotArticles.getTotal() == 0);


        /*测试返回的数量 start 2 end 4*/
        wbHotArticles = iGaia.getWbHotArticles(weiboDefaultMedia, 2, 4);
        assertTrue(wbHotArticles.getArticles().size() == 2);
        assertTrue(wbHotArticles.getTotal() > 0);

        wbHotArticles = iGaia.getWbHotArticles(weiboDefaultMedia, 0, 30);
        wbHotArticles.getArticles().stream().reduce((k1, k2) -> {
            int i = k1.getZpzNum().compareTo(k2.getZpzNum());
            assertTrue(k1.getZpzNum() >= k2.getZpzNum());
            return k2;
        });
        assertTrue(wbHotArticles.getTotal() > 0);
    }

    @Test
    public void getWbRecentArticles() throws Exception {
  /*测试返回的数量 start 0 end 0*/
        GArticleSet<GWbArticle> wbHotArticles = iGaia.getWbRecentArticles(weiboDefaultMedia, 0, 0);
        assertTrue(wbHotArticles.getArticles().size() == 0);
        assertTrue(wbHotArticles.getTotal() > 0);

        /*测试返回的数量 start 0 end -1*/
        wbHotArticles = iGaia.getWbRecentArticles(weiboDefaultMedia, 0, -1);
        assertTrue(wbHotArticles.getArticles().size() == 0);
        assertTrue(wbHotArticles.getTotal() > 0);

        /*测试返回的数量 start 0 end 3*/
        wbHotArticles = iGaia.getWbRecentArticles(weiboDefaultMedia, 0, 3);
        assertTrue(wbHotArticles.getArticles().size() == 3);
        assertTrue(wbHotArticles.getTotal() > 0);

        /*测试返回的数量 pmid为空的情况*/
        wbHotArticles = iGaia.getWbRecentArticles(null, 0, 3);
        assertTrue(wbHotArticles.getArticles().size() == 0);
        assertTrue(wbHotArticles.getTotal() == 0);


        /*测试返回的数量 pmid为没查到的情况的情况*/
        wbHotArticles = iGaia.getWbRecentArticles(Arrays.asList("fsdafsdf"), 0, 3);
        assertTrue(wbHotArticles.getArticles().size() == 0);
        assertTrue(wbHotArticles.getTotal() == 0);


        /*测试返回的数量 start 2 end 4*/
        wbHotArticles = iGaia.getWbRecentArticles(weiboDefaultMedia, 2, 4);
        assertTrue(wbHotArticles.getArticles().size() == 2);
        assertTrue(wbHotArticles.getTotal() > 0);

        wbHotArticles = iGaia.getWbRecentArticles(weiboDefaultMedia, 0, 30);
        wbHotArticles.getArticles().stream().reduce((k1, k2) -> {
            assertTrue(k1.getPostTime() >= k2.getPostTime());
            assertTrue(k1.getLikeNum() >= 0);
            assertTrue(k1.getCommentNum() >= 0);
            assertTrue(k1.getForwardNum() >= 0);
            return k2;
        });
        assertTrue(wbHotArticles.getTotal() > 0);

        int count2 = iGaia.getWbRecentArticleCount(weiboDefaultMedia);
        assertTrue(count2 > 0);
    }

    @Test
    public void getWxRecentArticles() throws Exception {
   /*测试返回的数量 start 0 end 0*/
        GArticleSet<GWxArticle> wxHotArticles = iGaia.getWxRecentArticles(weixinDefaultMedia, 0, 0);
        assertTrue(wxHotArticles.getArticles().size() == 0);
        assertTrue(wxHotArticles.getTotal() > 0);

        /*测试返回的数量 start 0 end -1*/
        wxHotArticles = iGaia.getWxRecentArticles(weixinDefaultMedia, 0, -1);
        assertTrue(wxHotArticles.getArticles().size() == 0);
        assertTrue(wxHotArticles.getTotal() > 0);

        /*测试返回的数量 start 0 end 3*/
        wxHotArticles = iGaia.getWxRecentArticles(weixinDefaultMedia, 0, 3);
        assertTrue(wxHotArticles.getArticles().size() == 3);
        assertTrue(wxHotArticles.getTotal() > 0);

        /*测试返回的数量 pmid为空的情况*/
        wxHotArticles = iGaia.getWxRecentArticles(null, 0, 3);
        assertTrue(wxHotArticles.getArticles().size() == 0);
        assertTrue(wxHotArticles.getTotal() == 0);


        /*测试返回的数量 pmid为没查到的情况的情况*/
        wxHotArticles = iGaia.getWxRecentArticles(Arrays.asList("fsdafsdf"), 0, 3);
        assertTrue(wxHotArticles.getArticles().size() == 0);
        assertTrue(wxHotArticles.getTotal() == 0);


        /*测试返回的数量 start 2 end 4*/
        wxHotArticles = iGaia.getWxRecentArticles(weixinDefaultMedia, 2, 4);
        assertTrue(wxHotArticles.getArticles().size() == 2);
        assertTrue(wxHotArticles.getTotal() > 0);

             /*测试返回的数量 start 2 end 4*/
        wxHotArticles = iGaia.getWxRecentArticles(weixinDefaultMedia, 2, 100000);
        assertTrue(wxHotArticles.getArticles().size() <= 200);
        assertTrue(wxHotArticles.getTotal() > 0);

        wxHotArticles = iGaia.getWxRecentArticles(weixinDefaultMedia, 0, 30);
        wxHotArticles.getArticles().stream().reduce((k1, k2) -> {
            assertTrue(k1.getPostTime() >= k2.getPostTime());
            assertTrue(k1.getReadNum() >= 0);
            assertTrue(k1.getLikeNum() >= 0);
            return k2;
        });
        assertTrue(wxHotArticles.getTotal() > 0);
    }


    @Test
    public void performTest() {
        ExecutorService executorService = Executors.newFixedThreadPool(8);
        long startTime = System.currentTimeMillis();
        int count = 2000;
        AtomicInteger i = new AtomicInteger(0);
        int n = 10;
        while (n-- > 0) {
            executorService.execute(() -> {
                while (i.get() < count) {
                    System.out.println("i 的值 :" + i);
                    GArticleSet<GWbArticle> wxHotArticles = iGaia.getWbRecentArticles(weiboDefaultMedia, 0, 10);
                    i.incrementAndGet();
                }

                long endTime = System.currentTimeMillis();
                System.out.println(String.format("%f/s cost %d's", count / ((endTime - startTime) / 1000.0), endTime - startTime));
            });
        }

        while (i.get() < count) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}