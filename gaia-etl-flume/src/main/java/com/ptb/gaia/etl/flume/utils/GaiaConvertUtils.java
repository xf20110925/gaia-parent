package com.ptb.gaia.etl.flume.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.hankcs.hanlp.HanLP;
import com.ptb.gaia.etl.flume.common.ArticleBrandFilter;
import com.ptb.gaia.etl.flume.entity.ArticleBrandBean;
import com.ptb.gaia.etl.flume.entity.ArticleBrandStatic;
import com.ptb.gaia.service.entity.article.*;
import com.ptb.gaia.service.entity.media.*;
import com.ptb.gaia.tokenizer.JSegBrandTextAnalyser;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by eric on 16/6/23.
 */
public class GaiaConvertUtils {

    static Logger logger = LoggerFactory.getLogger(GaiaConvertUtils.class);


    public static GWxMediaBasic convertJSONObjectToGWxMediaBasic(JSONObject jsonObject) {
        GWxMediaBasic gWxMediaBasic = new GWxMediaBasic();
        gWxMediaBasic.setBrief(jsonObject.getString("brief"));
        gWxMediaBasic.setAuthInfo(jsonObject.getString("authentication"));
        gWxMediaBasic.setIsAuth(StringUtils.isNotBlank(jsonObject.getString("authentication")) ? 1 : 0);
        gWxMediaBasic.setMediaName(jsonObject.getString("mediaName"));
        String weixinId = jsonObject.getString("weixinId");
        gWxMediaBasic.setWeixinId(weixinId);
        gWxMediaBasic.setPmid(jsonObject.getString("biz"));
        gWxMediaBasic.setAddTime(System.currentTimeMillis());
        gWxMediaBasic.setHeadImage(String.format("http://open.weixin.qq.com/qr/code/?username=%s", weixinId));
        gWxMediaBasic.setQrcode(String.format("http://open.weixin.qq.com/qr/code/?username=%s", weixinId));
        return gWxMediaBasic;
    }

    public static GWbMediaBasic convertJSONObjectToGWbMediaBasic(JSONObject jsonObject) {
        GWbMediaBasic gWbMediaBasic = new GWbMediaBasic();
        gWbMediaBasic.setPmid(jsonObject.getString("weiboId"));
        gWbMediaBasic.setHeadImage(jsonObject.getString("headImg"));
        gWbMediaBasic.setAddTime(System.currentTimeMillis());

        gWbMediaBasic.setMediaName(jsonObject.getString("mediaName"));
        gWbMediaBasic.setRegiterTime(jsonObject.getLong("registerTime") * 1000);
        gWbMediaBasic.setGender(jsonObject.getString("gender").startsWith("m") ? GWbMediaBasic.Male : GWbMediaBasic.Fmale);
        gWbMediaBasic.setLocation(jsonObject.getString("location"));
        gWbMediaBasic.setBrief(jsonObject.getString("brief"));
        String authType = jsonObject.getString("authType");
        String authDescription = jsonObject.getString("authDescription");
        gWbMediaBasic.setAuthInfo(authDescription);
        if (StringUtils.isBlank(authDescription)) {
            authType = "";
        }

        switch (authType) {
            case "":
            case "-1":
                gWbMediaBasic.setIsAuth(0);
                break;
            case "0":
                gWbMediaBasic.setIsAuth(1);
                break;
            default:
                gWbMediaBasic.setIsAuth(2);
                break;
        }

        return gWbMediaBasic;
    }

    static Pattern idxPattern = Pattern.compile(".*idx=(\\d+).*");

    public static Integer extractIdx(String input) {
        Matcher matcher = idxPattern.matcher(input);
        if (matcher.find()) {
            String idx = matcher.group(1);
            return Integer.parseInt(idx);
        }
        return null;
    }

    private static List<HotToken> formatHotToken(JSONArray kwList) {
        try {
            return kwList.stream().map(kw -> {
                String[] split = ((String) kw).split("/");
                HotToken hotToken = new HotToken();
                hotToken.setToken(split[0]);
                hotToken.setScore(split[1].split("\\.")[0]);
                return hotToken;
            }).collect(Collectors.toList());
        } catch (Exception e) {
            return null;
        }
    }

    public static GWxArticleBasic convertJSONObjectToGWxArticleBasic(JSONObject jsonObject) {
        GWxArticleBasic wxArticleBasic = new GWxArticleBasic();
        wxArticleBasic.setContent(jsonObject.getString("content"));
        wxArticleBasic.setTitle(jsonObject.getString("title"));
        wxArticleBasic.setAuthor(jsonObject.getString("author"));
        wxArticleBasic.setIsOriginal(jsonObject.getBoolean("original") == true ? 1 : 0);
        wxArticleBasic.setArticleUrl(jsonObject.getString("url"));
        wxArticleBasic.setPostTime(jsonObject.getLong("postTime") * 1000);
        wxArticleBasic.setPmid(jsonObject.getString("biz"));
        wxArticleBasic.setCoverPlan(jsonObject.getString("surface"));
        wxArticleBasic.setPosition(extractIdx(jsonObject.getString("url")));
        wxArticleBasic.setAddTime(System.currentTimeMillis());
        JSONArray keywords = jsonObject.getJSONArray("keywords");
        List<HotToken> htList = formatHotToken(keywords);
        wxArticleBasic.setHotToken(htList);
        wxArticleBasic.setLikeNum(-1);
        wxArticleBasic.setReadNum(-1);
        return wxArticleBasic;
    }

    public static List<ArticleBrandBean> convertJSONObjectToArticleBrand(JSONObject jsonObject, Map<String, Long> brandDirMap, Map<Long, List<String>> brandKeyWordsMap,
                                                                         Map<String, Long> categoryDirMap, Map<Long, List<String>> categoryKeyWordsMap, Map<Long, List<Long>> categoryBrandRel, String time) throws ConfigurationException {
        ArticleBrandBean brandBean = new ArticleBrandBean();
        Map<Long, Integer> sortMap = new LinkedHashMap<>();
        Map<Long, Integer> categoryCountMap = new HashMap<>();
        Map<Long, Integer> brandCountMap = new HashMap<>();
        List<Long> brandIdResult;

        if (categoryCountMap.isEmpty() || brandCountMap.isEmpty()) {
            initCountMap(brandDirMap, categoryDirMap, categoryCountMap, brandCountMap);
        }

        String content = jsonObject.getString("title").concat(",").concat(Jsoup.parseBodyFragment(jsonObject.getString("content")).text()).toLowerCase();

        if (Strings.isNullOrEmpty(content)) {
            return Collections.singletonList(brandBean);
        }

        //List<String> keywordList = Arrays.asList("这个,熟悉,音乐,曲子,笃雅行书,旋律,283449400,qq,欢迎,钢琴,投稿,望着,风沙,远去,结局,悲伤,什么样,无法,在我心中,抹去,累积,承诺,不能,面对,慢慢的,在最,将会,自己,记忆,见面".split(","));
//        List<String> keywordList = JSegBrandTextAnalyser.i().getKeyWords(content).stream().collect(Collectors.toList());
        List<String> keywordList = HanLP.extractKeyword(content, 30);
        categoryKeyWordsMap.entrySet().forEach(x -> {
            Long key = x.getKey();
            List<String> listSearch = x.getValue();
            keywordList.forEach(kw -> {
                if (listSearch.contains(kw)) {
                    categoryCountMap.put(key, categoryCountMap.get(key) + 1);
                }
            });
        });

        categoryCountMap.entrySet().stream().filter(x -> x.getValue() > 0).sorted(Map.Entry.<Long, Integer>comparingByValue().reversed()).forEachOrdered(x -> sortMap.put(x.getKey(), x.getValue()));

        if (!sortMap.isEmpty()) {
            List<Long> brandList = categoryBrandRel.get(sortMap.entrySet().stream().findFirst().get().getKey());
            if (CollectionUtils.isEmpty(brandList)) {
                return Collections.singletonList(brandBean);
            }
            brandIdResult = ArticleBrandFilter.BrandFilter(brandList, brandKeyWordsMap, brandCountMap, keywordList);
        } else {
            List<Long> brandList = brandDirMap.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());
            brandIdResult = ArticleBrandFilter.BrandFilter(brandList, brandKeyWordsMap, brandCountMap, keywordList);
        }

        String url = jsonObject.getString("url");

        List<ArticleBrandBean> resultList = new ArrayList<ArticleBrandBean>();
        if (CollectionUtils.isNotEmpty(brandIdResult)) {
            if (Objects.equals(brandIdResult.get(0), ArticleBrandStatic.Other)) {
                return Collections.singletonList(brandBean);
            }
            logger.info(String.format("URL >> [%s]", jsonObject.getString("url")));
            brandIdResult.forEach(x -> {
                ArticleBrandBean resultBrandBean = new ArticleBrandBean();
                resultBrandBean.setTime(time);
                resultBrandBean.setBrandId(x);
                resultBrandBean.setPmid(jsonObject.getString("biz"));
                resultBrandBean.setActualUrl(url);
                resultBrandBean.setUrl(DigestUtils.md5Hex(url));
                resultList.add(resultBrandBean);
            });
        }

        categoryCountMap.clear();
        brandCountMap.clear();
        return resultList;
    }


    public static GWxArticleDynamic convertJSONObjectToGWxArticleDynamic(JSONObject jsonObject) {
        GWxArticleDynamic wxArticleDynamic = new GWxArticleDynamic();
        wxArticleDynamic.setLikeNum(jsonObject.getIntValue("likes"));
        wxArticleDynamic.setReadNum(jsonObject.getIntValue("reads"));
        wxArticleDynamic.setArticleUrl(jsonObject.getString("url"));
        wxArticleDynamic.setAddTime(jsonObject.getLong("time"));
        return wxArticleDynamic;
    }

    public static void initCountMap(Map<String, Long> brandDirMap, Map<String, Long> categoryDirMap, Map<Long, Integer> categoryCountMap, Map<Long, Integer> brandCountMap) {
        brandDirMap.entrySet().forEach(x -> brandCountMap.put(x.getValue(), 0));
        categoryDirMap.entrySet().forEach(x -> categoryCountMap.put(x.getValue(), 0));
    }

    public static GWbArticleBasic convertJSONObjectToGWbArticleBasic(JSONObject jsonObject) {
        GWbArticleBasic wbArticleBasic = new GWbArticleBasic();
        wbArticleBasic.setContent(jsonObject.getString("content"));
        wbArticleBasic.setTitle(jsonObject.getString("title"));
        wbArticleBasic.setArticleUrl(jsonObject.getString("url"));
        wbArticleBasic.setSource(jsonObject.getString("source"));
        wbArticleBasic.setArticleType(jsonObject.getString("type"));
        wbArticleBasic.setPostTime(jsonObject.getLong("postTime") * 1000);
        wbArticleBasic.setCoverPlan(jsonObject.getString("surface"));
        wbArticleBasic.setAddTime(System.currentTimeMillis());
        wbArticleBasic.setHotToken(formatHotToken(jsonObject.getJSONArray("keywords")));
        wbArticleBasic.setPmid(jsonObject.getString("weiboId"));
        wbArticleBasic.setCoverPlan(jsonObject.getString("picture"));
        try {
            wbArticleBasic.setIsOriginal(jsonObject.getBoolean("original") ? 1 : 0);
        } catch (Exception e) {
            wbArticleBasic.setIsOriginal(0);
        }

        return wbArticleBasic;
    }

    public static GWbArticleDynamic convertJSONObjectToGWbArticleDynamic(JSONObject jsonObject) {
        GWbArticleDynamic wbArticleDynamic = new GWbArticleDynamic();
        wbArticleDynamic.setLikeNum(jsonObject.getInteger("likes"));
        wbArticleDynamic.setCommentNum(jsonObject.getInteger("comments"));
        wbArticleDynamic.setForwardNum(jsonObject.getInteger("forwards"));
        wbArticleDynamic.setArticleUrl(jsonObject.getString("url"));
        wbArticleDynamic.setAddTime(jsonObject.getLong("time"));
        wbArticleDynamic.setReadNum(jsonObject.getInteger("reads") == null ? 0 : jsonObject.getInteger("reads"));
        return wbArticleDynamic;
    }


    public static GLiveMediaCase convertJsonObjecToGLiveMediaStatic(JSONObject jsonObject) {
        GLiveMediaCase gl = new GLiveMediaCase();
        gl.setViewNum(jsonObject.getInteger("viewNum"));
        gl.setLikeNum(jsonObject.getInteger("likeNum"));
        gl.setCommentNum(jsonObject.getInteger("commentNum"));
        gl.setVideoUrl(jsonObject.getString("videoUrl"));
        gl.setAccountId(jsonObject.getString("accountId"));
        gl.setCoverPlan(jsonObject.getString("coverPlan"));
        gl.setPlat(jsonObject.getInteger("plat"));
        gl.setPlatIcon(jsonObject.getInteger("platIcon"));
        gl.setPostTime(jsonObject.getLong("postTime"));
        gl.setVideoTitle(jsonObject.getString("videoTitle"));
        gl.setPmid(jsonObject.getString("pmid"));
        gl.setScid(jsonObject.getString("scid"));
        gl.setAddTime(System.currentTimeMillis());
        gl.setHeadImage(null);
        gl.setMediaName(null);
        gl.setBrief(null);
        gl.setCategory(null);
        return gl;

    }

    public static GWbMediaBasic convertJSONObjectToGWbMediaDynamicBasic(JSONObject jsonObject) {
        GWbMediaBasic gWbMediaBasic = new GWbMediaBasic();
        gWbMediaBasic.setFansNum(jsonObject.getInteger("fans"));
        gWbMediaBasic.setPostNum(jsonObject.getInteger("postArticles"));
        gWbMediaBasic.setPmid(jsonObject.getString("weiboId"));
        return gWbMediaBasic;
    }

    public static GWxMediaBasic convertJSONObjectToGWxMediaDynamicBasic(JSONObject jsonObject) {
        GWxMediaBasic gWxMediaBasic = new GWxMediaBasic();
        gWxMediaBasic.setFansNum(jsonObject.getInteger("fans"));
        gWxMediaBasic.setWeixinId(jsonObject.getString("weixinId"));
        return gWxMediaBasic;
    }

    public static GLiveMediaStatic convertJSONobjectLiveMediaStatic(JSONObject jsonObject) {
        GLiveMediaStatic liveMediaStatic = new GLiveMediaStatic();
        liveMediaStatic.setPlat(jsonObject.getInteger("plat"));
        liveMediaStatic.setMediaName(jsonObject.getString("mediaName"));
        liveMediaStatic.setHeadImage(jsonObject.getString("headImage"));
        liveMediaStatic.setPlatIcon(jsonObject.getInteger("platIcon"));
        liveMediaStatic.setIsVip(jsonObject.getInteger("isVip"));
        liveMediaStatic.setFansNum(jsonObject.getInteger("fans"));
        liveMediaStatic.setIntroduction(jsonObject.getString("introduction"));
        liveMediaStatic.setLocation(jsonObject.getString("location"));
        liveMediaStatic.setAge(jsonObject.getString("age"));
        liveMediaStatic.setAccountLevel(jsonObject.getString("accountLevel"));
        liveMediaStatic.setGender(jsonObject.getInteger("gender"));
        liveMediaStatic.setAccountId(jsonObject.getString("accountId"));
        liveMediaStatic.setHomePage(jsonObject.getString("homePage"));
        liveMediaStatic.setDetailUrl(jsonObject.getString("detailUrl"));
        liveMediaStatic.setLiveMedia(jsonObject.getString("liveMedia"));
        liveMediaStatic.setVerificationInfo(jsonObject.getString("verificationInfo"));
        liveMediaStatic.setPmid(jsonObject.getString("pmid"));
        liveMediaStatic.setAddTime(System.currentTimeMillis());
        return liveMediaStatic;
    }

    public static GLiveMediaStatic convertJSONobjectLiveMediaDynamic(JSONObject jsonObject) {
        GLiveMediaStatic liveMediaStatic = new GLiveMediaStatic();
        liveMediaStatic.setMaxLiveUserNum(jsonObject.getInteger("maxLiveUserNum"));
        liveMediaStatic.setAvgLiveUserNum(jsonObject.getInteger("avgLiveUserNum"));
        liveMediaStatic.setTotalReward(jsonObject.getDouble("totalReward"));
        liveMediaStatic.setTotalAmount(jsonObject.getDouble("totalAmount"));
        liveMediaStatic.setTotalLikeNum(jsonObject.getInteger("totalLikeNum"));
        liveMediaStatic.setPmid(jsonObject.getString("pmid"));
        liveMediaStatic.setFansData(JSON.parseArray(jsonObject.getString("fansData"), Point.class));
        return liveMediaStatic;
    }

    public static GVedioMediaStatic convertJSONobjectVedioMediaStatic(JSONObject jsonObject) {
        GVedioMediaStatic gVedioMediaStatic = new GVedioMediaStatic();
        gVedioMediaStatic.setPlat(jsonObject.getInteger("plat"));
        gVedioMediaStatic.setMediaName(jsonObject.getString("mediaName"));
        gVedioMediaStatic.setLocation(jsonObject.getString("location"));
        gVedioMediaStatic.setHomePage(jsonObject.getString("homePage"));
        gVedioMediaStatic.setAdvantage(jsonObject.getString("advantage"));
        gVedioMediaStatic.setAccountId(jsonObject.getString("accountId"));
        gVedioMediaStatic.setFansNum(jsonObject.getInteger("fans"));
        gVedioMediaStatic.setGender(jsonObject.getInteger("gender"));
        gVedioMediaStatic.setLiveMedia(jsonObject.getString("liveMedia"));
        return gVedioMediaStatic;
    }

//    public static void main(String[] args) throws ConfigurationException {
//
////        List<Integer> list = JSegBrandTextAnalyser.i().getKeyWords("我爱北京太南门，中国最牛逼 钓鱼岛是中国额").stream().map(String::hashCode).collect(Collectors.toList());
////
////        System.out.println("123");
//    }

}
