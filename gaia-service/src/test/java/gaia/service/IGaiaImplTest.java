package gaia.service;

import com.ptb.gaia.service.IGaia;
import com.ptb.gaia.service.IGaiaImpl;
import com.ptb.gaia.service.RankType;
import com.ptb.gaia.service.entity.media.*;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertTrue;

/**
 * Created by eric on 16/6/30.
 */
public class IGaiaImplTest {
    IGaia iGaia = new IGaiaImpl();

    List<String> wbpmid = Arrays.asList("1764222885", "1266321801", "2714280233");
    String mediaPmid = "1764222885";


    public IGaiaImplTest() throws ConfigurationException {
    }


    private void checkGWxMedia(GWxMedia gWxMedia) {
        assertTrue(gWxMedia.getIsAuth() >= 0);
        assertTrue(StringUtils.isNotBlank(gWxMedia.getBrief()));
        assertTrue(StringUtils.isNotBlank(gWxMedia.getHeadImage()));
        assertTrue(StringUtils.isNoneBlank(gWxMedia.getMediaName()));
        assertTrue(StringUtils.isNoneBlank(gWxMedia.getQrcode()));
        assertTrue(gWxMedia.getIsOriginal() >= 0);
    }

    @Test
    public void getWxMedia() throws Exception {
//        //查询查询不到ID的情况
//        GWxMedia ddd = iGaia.getWxMedia("ddd");
//        assertTrue(ddd == null);

        //查询查询到ID的情况
        GWxMedia fff = iGaia.getWxMedia("MjM5MjEzNzYzOA==");
        assertTrue(fff != null);

        //检查返回的字段
        checkGWxMedia(fff);

        //参数异常的情况

    }


    /*
         Get One Media detail!!
         menghcen 2016-7-4
         Test Media : (当时我就震惊了,姚晨,papi酱)
     */
    @Test
    public void getWbMedia() throws Exception {

        //当pmid不存在
        GWbMedia pmid = iGaia.getWbMedia("1700000000");
        assertTrue(pmid == null);

        //当pmid不存在 当传入值为null
        GWbMedia pmid1 = iGaia.getWbMedia(null);
        assertTrue(pmid1 == null);

        //当pmid存在   1764222885 当时我就震惊了
        GWbMedia ZhenJingWbMedia = iGaia.getWbMedia(mediaPmid);
        checkGWbMedia(ZhenJingWbMedia);

    }

    private void checkGWbMedia(GWbMediaBasic gWbMedia) {
        assertTrue(gWbMedia.getIsAuth() >= 0);
        assertTrue(gWbMedia.getGender() >= 0);
//        assertTrue(StringUtils.isNotBlank(gWbMedia.getBrief()));
        assertTrue(StringUtils.isNotBlank(gWbMedia.getHeadImage()));
        assertTrue(StringUtils.isNotBlank(gWbMedia.getMediaName()));
        assertTrue(StringUtils.isNotBlank(gWbMedia.getAuthInfo()));
    }

    @Test
    public void getWxMediaBatch() throws Exception {
        //pmid不存在的情况
        List<String> bizs = Arrays.asList("MjM5MjEzNzYzOA==", "ddd");
        List<GWxMedia> testMedias = iGaia.getWxMediaBatch(bizs);
        assertTrue(testMedias.size() == 0);
        //pmid 存在
        bizs = Arrays.asList("MzAxNzc2NjE1NA==", "MzAwNDc3MzU3Mw==", "MzIyMTE0NDM2MQ==");
        List<GWxMedia> medias = iGaia.getWxMediaBatch(bizs);
        assertTrue(medias.size() == 3);
        medias.forEach(this::checkGWxMedia);
    }


    /*
         Get Batch Media detail!!
         menghcen 2016-7-4
         Test Media : (当时我就震惊了,姚晨,papi酱)
     */
    @Test
    public void getWbMediaBatch() throws Exception {

        // pmid存在  传入其中一个pmid 没有
        // 1764222885  当时我就震惊了
        // 1266321801  姚晨
        // 170000000	测试没有数据
        List<String> pmids1 = Arrays.asList("1764222885", "1266321801", "170000000");
        List<GWbMedia> medias1 = iGaia.getWbMediaBatch(pmids1);
        assertTrue(medias1.size() > 0);
        medias1.forEach(this::checkGWbMedia);

        // pmid存在  传入的pmid都存在
        // 1764222885  当时我就震惊了
        // 1266321801  姚晨
        // 2714280233	papi酱
        List<String> pmids2 = Arrays.asList("1764222885", "1266321801", "2714280233");
        List<GWbMedia> medias2 = iGaia.getWbMediaBatch(pmids2);
        assertTrue(medias2.size() > 0);
        medias2.forEach(this::checkGWbMedia);

        // pmid存在  传入的pmid 其中一个为 null
        // 1764222885  当时我就震惊了
        // 1266321801  姚晨
        // 2714280233	papi酱
        List<String> pmids3 = Arrays.asList("1764222885", null, "2714280233");
        List<GWbMedia> medias3 = iGaia.getWbMediaBatch(pmids3);
        assertTrue(medias3.size() > 0);
        medias3.forEach(this::checkGWbMedia);

    }

    @Test
    public void getWxDynamics() throws Exception {
    }

    @Test
    public void getWxHotArticles() throws Exception {

    }

    @Test
    public void getWbHotArticles() throws Exception {

    }

    @Test
    public void getWbRecentArticles() throws Exception {

    }

    @Test
    public void getWxRecentArticles() throws Exception {

    }

    @Test
    public void getWxCapabilityMedia() throws Exception {
        //bizs为空
        List<String> bizs = new ArrayList<>();
        GMediaSet<GWxMedia> emptyMediaSet = iGaia.getWxCapabilityMedia(bizs, 0, 4, RankType.RANK_HEAD_VERG_READ_RATE);
        assertTrue(emptyMediaSet.getTotal() == 0 && emptyMediaSet.getMedias().isEmpty());
        //bizs为null
        GMediaSet<GWxMedia> nullMediaSet = iGaia.getWxCapabilityMedia(null, 0, 4, RankType.RANK_HEAD_VERG_READ_RATE);
        assertTrue(nullMediaSet.getTotal() == 0 && nullMediaSet.getMedias().isEmpty());
        //bizs不为空
        bizs = Arrays.asList("MjM5NDcyNDAzNA==", "MjM5NzQ5MTkyMA==", "MjM5NzAxNTkzNg==", "MzA5ODc0MTUwNA==");
        // start=0, end=0
        GMediaSet<GWxMedia> readRateCapMediaSet = iGaia.getWxCapabilityMedia(bizs, 0, 0, RankType.RANK_HEAD_VERG_READ_RATE);
        assertTrue(readRateCapMediaSet.getTotal() <= 4 && readRateCapMediaSet.getTotal() >= 0);
        assertTrue(readRateCapMediaSet.getMedias().size() == 0);

        //start=0, end=-1
        readRateCapMediaSet = iGaia.getWxCapabilityMedia(bizs, 0, -1, RankType.RANK_HEAD_VERG_READ_RATE);
        assertTrue(readRateCapMediaSet.getTotal() <= 4 && readRateCapMediaSet.getTotal() >= 0);
        assertTrue(readRateCapMediaSet.getMedias().size() == 0);

        //头条平均阅读数增长率
        readRateCapMediaSet = iGaia.getWxCapabilityMedia(bizs, 0, 4, RankType.RANK_HEAD_VERG_READ_RATE);
        assertTrue(readRateCapMediaSet.getTotal() <= 4);
        assertTrue(readRateCapMediaSet.getMedias().stream().allMatch(wxMedia -> wxMedia.getAvgHeadReadRateInPeroid() != -1000000));
        readRateCapMediaSet.getMedias().stream().reduce((m1, m2) -> {
            assertTrue(m1.getAvgHeadReadRateInPeroid() >= m2.getAvgHeadReadRateInPeroid());
            assertTrue(m1.getAvgHeadReadRateRankInPeroid() <= m2.getAvgHeadReadRateRankInPeroid());
            return m2;
        });

        //头条平均点赞数增长率
        GMediaSet<GWxMedia> likeRateCapMediaSet = iGaia.getWxCapabilityMedia(bizs, 0, 4, RankType.RANK_HEAD_VERG_LIKE_RATE);
        assertTrue(likeRateCapMediaSet.getTotal() <= 4);
        likeRateCapMediaSet.getMedias().stream().allMatch(wxMedia -> wxMedia.getAvgHeadReadRateInPeroid() != -1000000);
        likeRateCapMediaSet.getMedias().stream().reduce((m1, m2) -> {
            assertTrue(m1.getAvgHeadLikeRateInPeroid() >= m2.getAvgHeadLikeRateInPeroid());
            assertTrue(m1.getAvgHeadLikeRateRankInPeroid() <= m1.getAvgHeadLikeRateRankInPeroid());
            return m2;
        });
    }

    @Test
    public void getWbCapabilityMedia() throws Exception {

        List<String> bizs = new ArrayList<>();
        List<GMediaSet> list = wbMediaRatioTest(bizs, 0, 3);
        assertTrue(list.get(0).getMedias().size() == 0);
        assertTrue(list.get(1).getMedias().size() == 0);
        assertTrue(list.get(2).getMedias().size() == 0);

        List<GMediaSet> list2 = wbMediaRatioTest(wbpmid, 0, 3);
        assertTrue(list.get(0).getMedias().size() <= 3);
        assertTrue(list.get(1).getMedias().size() <= 3);
        assertTrue(list.get(2).getMedias().size() <= 3);

        //comment
        List<GWbMedia> commentlist = list2.get(0).getMedias();
        assertTrue(commentlist.stream().allMatch(wxMedia -> wxMedia.getAvgCommentRateInPeroid() != -1000000));
        commentlist.stream().reduce((m1, m2) -> {
            assertTrue(m1.getAvgCommentRateInPeroid() >= m2.getAvgCommentRateInPeroid());
            assertTrue(m1.getAvgCommentRateRankInPeroid() <= m2.getAvgCommentRateRankInPeroid());
            return m2;
        });

        //like
        List<GWbMedia> likeslist = list2.get(1).getMedias();
        assertTrue(likeslist.stream().allMatch(wxMedia -> wxMedia.getAvgLikeRateInPeroid() != -1000000));
        likeslist.stream().reduce((m1, m2) -> {
            assertTrue(m1.getAvgLikeRateInPeroid() >= m2.getAvgLikeRateInPeroid());
            assertTrue(m1.getAvgLikeRateRankInPeroid() <= m2.getAvgLikeRateRankInPeroid());
            return m2;
        });

        //forward
        List<GWbMedia> forwardlist = list2.get(2).getMedias();
        assertTrue(forwardlist.stream().allMatch(wxMedia -> wxMedia.getAvgForwardRateInPeroid() != -1000000));
        forwardlist.stream().reduce((m1, m2) -> {
            assertTrue(m1.getAvgForwardRateInPeroid() >= m2.getAvgForwardRateInPeroid());
            assertTrue(m1.getAvgForwardRateRankInPeroid() <= m2.getAvgForwardRateRankInPeroid());
            return m2;
        });

        // 0 -1
        List<GMediaSet> list3 = wbMediaRatioTest(wbpmid, 0, -1);
        assertTrue(list3.get(0).getMedias().size() == 0);
        assertTrue(list3.get(1).getMedias().size() == 0);
        assertTrue(list3.get(2).getMedias().size() == 0);

        //0 0
        List<GMediaSet> list4 = wbMediaRatioTest(wbpmid, 0, 0);
        assertTrue(list4.get(0).getMedias().size() == 0);
        assertTrue(list4.get(1).getMedias().size() == 0);
        assertTrue(list4.get(2).getMedias().size() == 0);

    }


    @Test
    public void getWxHotMedia() throws Exception {
        List<String> bizs = Arrays.asList("MzA4MDE4OTYxNA==", "MjM5NDcyNDAzNA==", "MjM5MjEzNzYzOA==", "MjM5NDMxMjM5Nw==", "MzA3NzQ3MjMwOA==");
        //头条平均阅读数
        GMediaSet<GWxMedia> readNumCapMediaSet = iGaia.getWxHotMedia(bizs, 0, 4, RankType.RANK_HEAD_VERG_READ);
        assertTrue(readNumCapMediaSet.getTotal() <= 4);
        readNumCapMediaSet.getMedias().stream().allMatch(wxMedia -> wxMedia.getAvgHeadReadNumInPeroid() > 0);
        readNumCapMediaSet.getMedias().stream().reduce((m1, m2) -> {
            assertTrue(m1.getAvgHeadReadNumInPeroid() >= m2.getAvgHeadReadNumInPeroid());
            return m2;
        });
        //头条平均点赞数
        GMediaSet<GWxMedia> likeNumCapMediaSet = iGaia.getWxHotMedia(bizs, 0, 4, RankType.RANK_HEAD_VERG_LIKE);
        assertTrue(likeNumCapMediaSet.getTotal() <= 4);
        likeNumCapMediaSet.getMedias().stream().allMatch(wxMedia -> wxMedia.getAvgHeadLikeNumInPeroid() > 0);
        likeNumCapMediaSet.getMedias().stream().reduce((m1, m2) -> {
            assertTrue(m1.getAvgHeadLikeNumInPeroid() >= m2.getAvgHeadLikeNumInPeroid());
            return m2;
        });

    }

    public List<GMediaSet> wbMediaRatioTest(List<String> pmid, int start, int end) {

        List<GMediaSet> list = new ArrayList<>();

        GMediaSet<GWbMedia> wbHotCommentMedia1 = iGaia.getWbHotMedia(pmid, start, end, RankType.RANK_VERG_COMMENT_RATE_RANK);

        GMediaSet<GWbMedia> wbHotLikesMedia1 = iGaia.getWbHotMedia(pmid, start, end, RankType.RANK_VERG_LIKE_RATE_RANK);

        GMediaSet<GWbMedia> wbHotForwardMedia1 = iGaia.getWbHotMedia(pmid, start, end, RankType.RANK_VERG_FORWARD_RATE_RANK);

        list.add(wbHotCommentMedia1);
        list.add(wbHotLikesMedia1);
        list.add(wbHotForwardMedia1);

        return list;

    }


    public List<GMediaSet> wbMediaTest(List<String> pmid, int start, int end) {

        List<GMediaSet> list = new ArrayList<>();

        GMediaSet<GWbMedia> wbHotCommentMedia1 = iGaia.getWbHotMedia(pmid, start, end, RankType.RANK_VERG_COMMENT);

        GMediaSet<GWbMedia> wbHotLikesMedia1 = iGaia.getWbHotMedia(pmid, start, end, RankType.RANK_VERG_LIKE);

        GMediaSet<GWbMedia> wbHotForwardMedia1 = iGaia.getWbHotMedia(pmid, start, end, RankType.RANK_VERG_FORWARD);

        list.add(wbHotCommentMedia1);
        list.add(wbHotLikesMedia1);
        list.add(wbHotForwardMedia1);

        return list;

    }


    @Test
    public void getWbHotMedia() throws Exception {
        //当传入数组值 有null值得 现象的时候
        //平均评论数
        GMediaSet<GWbMedia> wbHotCommentMedia = iGaia.getWbHotMedia(null, 0, 3, RankType.RANK_VERG_COMMENT);
        assertTrue(wbHotCommentMedia.getTotal() == 0);
        //平均点赞数
        GMediaSet<GWbMedia> wbHotLikesMedia = iGaia.getWbHotMedia(null, 0, 3, RankType.RANK_VERG_LIKE);
        assertTrue(wbHotLikesMedia.getTotal() == 0);
        //平均转发数
        GMediaSet<GWbMedia> wbHotForwardMedia = iGaia.getWbHotMedia(null, 0, 3, RankType.RANK_VERG_FORWARD);
        assertTrue(wbHotForwardMedia.getTotal() == 0);

        //当传进去的数据为  start:0   start:-1
        List<GMediaSet> list = wbMediaTest(wbpmid, 0, -1);
        assertTrue(list.get(0).getMedias().size() == 0);
        assertTrue(list.get(1).getMedias().size() == 0);
        assertTrue(list.get(2).getMedias().size() == 0);

        //当传进去的数据为
        List<GMediaSet> list2 = wbMediaTest(Arrays.asList("dawdwadwa"), 0, -1);
        assertTrue(list2.get(0).getMedias().size() == 0);
        assertTrue(list2.get(1).getMedias().size() == 0);
        assertTrue(list2.get(2).getMedias().size() == 0);

        //返回的数量是 start 2 end 5
        List<GMediaSet> list3 = wbMediaTest(wbpmid, 1, 5);
        assertTrue(list3.get(0).getMedias().size()  > 0);
        assertTrue(list3.get(1).getMedias().size() >  0);
        assertTrue(list3.get(2).getMedias().size()  > 1);

        //返回的数量的个数 小于 设置的范围
        List<GMediaSet> list4 = wbMediaTest(wbpmid, 0, 10);
        assertTrue(list4.get(0).getMedias().size() == wbpmid.size());
        assertTrue(list4.get(1).getMedias().size() == wbpmid.size());
        assertTrue(list4.get(2).getMedias().size() == wbpmid.size());

        //返回的数量的个数 小于 设置的范围
        List<GMediaSet> list5 = wbMediaTest(wbpmid, 0, 0);
        assertTrue(list5.get(0).getMedias().size() == 0);
        assertTrue(list5.get(1).getMedias().size() == 0);
        assertTrue(list5.get(2).getMedias().size() == 0);


        //返回的数量的个数 测试排名
        List<GMediaSet> list6 = wbMediaTest(wbpmid, 0, 10);

        //comment
        List<GWbMedia> commentlist = list6.get(0).getMedias();
        commentlist.stream().reduce((k1, k2) -> {
            int i = k1.getAvgCommentNumInPeroid().compareTo(k2.getAvgCommentNumInPeroid());
            assertTrue(k1.getAvgCommentNumInPeroid() >= k2.getAvgCommentNumInPeroid());
            return k2;
        });

        //like
        List<GWbMedia> forwardlist = list6.get(2).getMedias();
        forwardlist.stream().reduce((k1, k2) -> {
            int i = k1.getAvgForwardNumInPeroid().compareTo(k2.getAvgForwardNumInPeroid());
            assertTrue(k1.getAvgForwardNumInPeroid() >= k2.getAvgForwardNumInPeroid());
            return k2;
        });

        //forward
        List<GWbMedia> likelist = list6.get(1).getMedias();
        likelist.stream().reduce((k1, k2) -> {
            int i = k1.getAvgLikeNumInPeroid().compareTo(k2.getAvgLikeNumInPeroid());
            assertTrue(k1.getAvgLikeNumInPeroid() >= k2.getAvgLikeNumInPeroid());
            return k2;
        });
    }

    @Test
    public void addWxMediaBasicBatch() throws Exception {

    }

    @Test
    public void addWbMediaBasicBatch() throws Exception {

    }

    @Test
    public void addWbArticleDynamics() throws Exception {

    }

    @Test
    public void addWxArticleDynamics() throws Exception {

    }

    @Test
    public void addWxArticleBasicBatch() throws Exception {

    }

    @Test
    public void addWbArticleBasicBatch() throws Exception {

    }

    @Test
    public void getWbMediaByArticleUrl() {
        List<String> urls = Arrays.asList("http://weibo.com/2495327692/E0KWokkDy?ref=home&rid=0_0_1_2669732000262040755&type=comment", "http://weibo.com/libingbing?refer_flag=1001030101_", "http://m.weibo.cn/u/5187664653");
        urls.forEach(url -> {
            GWbMediaBasic wbMedia = iGaia.getWbMediaByArticleUrl(url);
            checkGWbMedia(wbMedia);
        });
    }

    @Test
    public void getWxMediaByArticleUrl() {
        GWxMediaBasic wxMedia = iGaia.getWxMediaByArticleUrl("http://mp.weixin.qq.com/s?__biz=MjM5MDAxNjkyMA==&mid=208185681&idx=5&sn=cd7cbddfb8dacf09bec42075aababe6f#rd");
        checkGWxMedia((GWxMedia) wxMedia);
    }

    @Test
    public void getLiveMediaBatchTest(){
        List<String> liveMedias = new ArrayList<>();
        liveMedias.add("322292067");
        liveMedias.add("5601774");
        //liveMedias.add("625678539");
        List<GLiveMediaStatic> liveMediaBatch = iGaia.getLiveMediaBatch(liveMedias);
        System.out.println(liveMediaBatch.size());
    }
}