package com.ptb.gaia.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.ptb.gaia.service.entity.article.*;
import com.ptb.gaia.service.entity.media.*;
import com.ptb.gaia.service.entity.article.GArticleSet;
import com.ptb.gaia.service.entity.article.GWbArticle;
import com.ptb.gaia.service.entity.article.GWbArticleBasic;
import com.ptb.gaia.service.entity.article.GWbArticleDynamic;
import com.ptb.gaia.service.entity.article.GWxArticle;
import com.ptb.gaia.service.entity.article.GWxArticleBasic;
import com.ptb.gaia.service.entity.article.GWxArticleDynamic;
import com.ptb.gaia.service.service.*;
import com.ptb.gaia.service.service.*;
import com.ptb.gaia.service.utils.ConvertUtils;
import com.ptb.uranus.common.entity.CollectType;
import com.ptb.uranus.schedule.model.Priority;
import com.ptb.uranus.schedule.trigger.JustOneTrigger;
import com.ptb.uranus.sdk.UranusSdk;
import com.ptb.uranus.spider.weibo.WeiboSpider;
import com.ptb.uranus.spider.weibo.bean.WeiboAccount;
import com.ptb.uranus.spider.weixin.WeixinSpider;
import com.ptb.uranus.spider.weixin.bean.WxAccount;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by eric on 16/6/23.
 */
public class IGaiaImpl implements IGaia {
	GWbMediaService gWbMediaService = new GWbMediaService();
	GWxMediaService gWxMediaService = new GWxMediaService();
	GWbArticleService gWbArticleService = new GWbArticleService();
	GWxArticleService gWxArticleService = new GWxArticleService();
	GRecommandService gRecommandService = new GRecommandService();
	GLiveMediaService gLiveMediaService = new GLiveMediaService();
	GVedioMediaService gVedioMediaService = new GVedioMediaService();

	public IGaiaImpl() throws ConfigurationException {
	}

	@Override
	public GWxMedia getWxMedia(String biz) {
		List<GWxMedia> wxMedias = gWxMediaService.getWxMedia(Arrays.asList(biz));
		if (wxMedias == null || wxMedias.size() == 0) {
			return null;
		}
		return wxMedias.get(0);
	}

	@Override
	public GWbMedia getWbMedia(String wbId) {
		List<GWbMedia> wbMedias = gWbMediaService.getWbMedia(Arrays.asList(wbId));
		if (wbMedias == null || wbMedias.size() == 0) {
			return null;
		}
		return wbMedias.get(0);
	}

	@Override
	public List<GWxMedia> getWxMediaBatch(List<String> bizs) {
		return gWxMediaService.getWxMedia(bizs);
	}

	@Override
	public List<GWbMedia> getWbMediaBatch(List<String> weiboids) {
		return gWbMediaService.getWbMedia(weiboids);
	}

	@Override
	public List<GWxArticleDynamic> getWxDynamics(String artilce, Long startTime, Long endTime) {
		return gWxArticleService.getDynamics(artilce, startTime, endTime);
	}


	@Override
	public GArticleSet<GWxArticle> getWxHotArticles(List<String> pmids, int start, int end) {
		return gWxArticleService.getHotArticles(pmids, start, end);
	}

	@Override
	public GArticleSet<GWbArticle> getWbHotArticles(List<String> pmids, int start, int end) {
		return gWbArticleService.getHotArticles(pmids, start, end);
	}

	@Override
	public GArticleSet<GWbArticle> getWbRecentArticles(List<String> pmids, int start, int end) {
		return gWbArticleService.getRecentArticles(pmids, start, end);
	}

	@Override
	public GArticleSet<GWxArticle> getWxRecentArticles(List<String> pmids, int start, int end) {
		return gWxArticleService.getRecentArticles(pmids, start, end);
	}

	@Override
	public int getWxRecentArticleCount(List<String> pmids) {
		return gWxArticleService.getWxRecentArticleCount(pmids);
	}

	@Override
	public int getWbRecentArticleCount(List<String> pmids) {
		return gWbArticleService.getWbRecentArticleCount(pmids);
	}

	public GMediaSet<GWxMedia> getWxCapabilityMedia(
			List<String> pmids, int start, int end, RankType rankType) {
		return gWxMediaService.getCapabilityMedia(pmids, start, end, rankType);

	}

	@Override
	public GMediaSet<GWbMedia> getWbCapabilityMedia(
			List<String> pmids, int start, int end, RankType rankType) {
		return gWbMediaService.getCapabilityMedia(pmids, start, end, rankType);
	}

	@Override
	public GMediaSet<GWxMedia> getWxHotMedia(
			List<String> pmids, int start, int end, RankType rankType) {
		return gWxMediaService.getHotMediaRank(pmids, start, end, rankType);
	}

	@Override
	public GMediaSet<GWbMedia> getWbHotMedia(
			List<String> pmids, int start, int end, RankType rankType) {
		return gWbMediaService.getHotMediaRank(pmids, start, end, rankType);
	}

	@Override
	public Boolean addWxMediaBasicBatch(List<GWxMediaBasic> wxMedias) {
		return gWxMediaService.insertBatch(wxMedias);
	}

	@Override
	public Boolean addWbMediaBasicBatch(List<GWbMediaBasic> wbMediaBasics) {
		return gWbMediaService.insertBatch(wbMediaBasics);
	}

	@Override
	public Boolean addWbArticleDynamics(List<GWbArticleDynamic> wbArticleDynamic) {
		return gWbArticleService.inserDynamicBatch(wbArticleDynamic);
	}

	@Override
	public Boolean addWxArticleDynamics(List<GWxArticleDynamic> wxArticleDynamic) {
		return gWxArticleService.insertDynamicBatch(wxArticleDynamic);
	}

	@Override
	public Boolean addWxArticleBasicBatch(List<GWxArticleBasic> wxArticleBasics) {
		return gWxArticleService.insertBasicBatch(wxArticleBasics);
	}

	@Override
	public Boolean addWbArticleBasicBatch(List<GWbArticleBasic> wbArticleBasics) {
		return gWbArticleService.insertBasicBatch(wbArticleBasics);
	}

	@Override
	public GWxMedia getWxMediaByArticleUrl(String articleUrl) {
		Pattern pattern = Pattern.compile(".*biz=([^&]*).*");
		Matcher matcher = pattern.matcher(articleUrl);
		GWxMedia wxMedia = null;
		String biz = null;
		if (matcher.find()) {
			biz = matcher.group(1);
			if (StringUtils.isNotBlank(biz)) wxMedia = getWxMedia(biz);
		}
		if (wxMedia == null) {
			WeixinSpider wxSpider = new WeixinSpider();
			Optional<WxAccount> wxAccountOpt = wxSpider.getWeixinAccountByArticleUrl(articleUrl);
			if (wxAccountOpt.isPresent()) {
				wxMedia = ConvertUtils.convert2GWxMedia(wxAccountOpt.get());
				gWxMediaService.insertBatch(Arrays.asList(wxMedia));
				UranusSdk.i().collect(articleUrl, CollectType.C_WX_M_S, new JustOneTrigger(), Priority.L1);
			}
		}
		return wxMedia;
	}

	@Override
	public GWbMedia getWbMediaByArticleUrl(String articleUrl) {
		Pattern pattern = Pattern.compile(".*(weibo\\.com|m\\.weibo\\.cn)/(\\d+)/.*");
		Matcher matcher = pattern.matcher(articleUrl);
		GWbMedia wbMedia = null;
		String wbId = null;
		if (matcher.find()) {
			wbId = matcher.group(2);
			if (StringUtils.isNotBlank(wbId)) {
				wbMedia = getWbMedia(wbId);
			}
		}
		if (wbMedia == null) {
			WeiboSpider spider = new WeiboSpider();
			WeiboAccount wbAccount = spider.getWeiboAccountByArticleUrl(articleUrl).orElse(spider.getWeiboAccountByHomePage(articleUrl).orElse(null));
			if (wbAccount != null) {
				wbMedia = ConvertUtils.convert2GWbMedia(wbAccount);
				gWbMediaService.insertBatch(Arrays.asList(wbMedia));
				if (StringUtils.isNotBlank(wbId))
					UranusSdk.i().collect(wbId, CollectType.C_WB_M_S, new JustOneTrigger(), Priority.L1);
			}
		}
		return wbMedia;
	}

	@Override
	public List<GWbArticle> getWbArticle(List<String> articleUrls) {
		return gWbArticleService.getArticle(articleUrls,GWbArticle.class);
	}

	@Override
	public List<GWxArticle> getWxArtcile(List<String> articleUrls) {
		return gWxArticleService.getArticle(articleUrls,GWxArticle.class);
	}

	@Override
	public Document getWxDocument(WxMediaStatistics wxMediaStatistics) {
		return Document.parse(JSONObject.toJSONString(wxMediaStatistics));
	}

	@Override
	public Document getWbDocument(WbMediaStatistics wbMediaStatistics) {
		return Document.parse(JSON.toJSONString(wbMediaStatistics));
	}

	@Override
	public Boolean addBatchCategory(
			List<RecommendCategoryMedia> recommendCategoryMedias) {
		return gRecommandService.addBatchCategory(recommendCategoryMedias);
	}

	@Override
	public void removeAllRecommandCategory() {
		gRecommandService.delAllRecoomandCategory();
	}

	@Override
	public ArrayList<Document> getCovertType(ArrayList<Document> list) {
		List<Document> resultList = new ArrayList<>();
		resultList = list.stream().map(x->{
				Long num = Long.valueOf(x.get("value").toString());
				x.put("value",num);
			return x;
		}).collect(Collectors.toList());

		return (ArrayList<Document>) resultList;
	}

	@Override
	public ArrayList<BasicDBObject> getListBasicObject(FindIterable<Document> docs) {

		ArrayList list = new ArrayList<DBObject>();

		for(Document doc : docs){
			list.add(new BasicDBObject(JSONObject.parseObject(JSON.toJSONString(doc),HashMap.class)));
		}

		return list;
	}

	@Override
	public Boolean addWxMediaDynamicBatch(List<GWxMediaBasic> gWxMediaBasics) {
		return gWxMediaService.insertDynamicBatch(gWxMediaBasics);
	}

	@Override
	public Boolean addLiveMediaBasicBatch(List<GLiveMediaStatic> gLiveMediaStatics) {

		return gLiveMediaService.insertBatch(gLiveMediaStatics);
	}

	@Override
	public Boolean addVedioMediaBasicBatch(List<GVedioMediaStatic> gVedioMediaStatics) {
		return gVedioMediaService.insertBatch(gVedioMediaStatics);
	}

	@Override
	public Boolean updateLiveMediaDynamicBatch(List<GLiveMediaStatic> gLiveMediaStatics) {
		return gLiveMediaService.updateBatch(gLiveMediaStatics);
	}

	@Override
	public List<GLiveMediaStatic> getLiveMediaBatch(List<String> pmidList){
		return gLiveMediaService.getLiveMediaBatch(pmidList);

	}

	@Override
	public Boolean addLiveArticleDynamicBatch(List<GLiveMediaCase> gLiveMediaCases) {
		return gLiveMediaService.insertArticleBatch(gLiveMediaCases);
	}

	@Override
	public List<String> getWxArticleByPmid(List<String> pmids) {
		return gWxArticleService.getWxArticleList(pmids);
	}

	@Override
	public Boolean updataWxMediaStatic(List<GWxMediaBasic> medias) {
		return gWxMediaService.updateManyModelWxMedia(medias);
	}

	public static void main(String[] args) throws ConfigurationException {
//		IGaiaImpl iGaia = new IGaiaImpl();
//	   /* GWbMediaBasic wbMedia = iGaia.getWbMediaByArticleUrl("http://weibo.com/u/1789834424?is_hot=1");
//		System.out.println(JSON.toJSONString(wbMedia));*/
//		GWxMediaBasic wxMedia = iGaia.getWxMediaByArticleUrl("http://mp.weixin.qq.com/s?__biz=MjM5MDAxNjkyMA==&mid=208185681&idx=5&sn=cd7cbddfb8dacf09bec42075aababe6f");
//		System.out.println(JSON.toJSONString(wxMedia));   1467763189884 1467763189886

		GWxArticleService wxAservice = new GWxArticleService();
		GWbArticleService wbAservice = new GWbArticleService();
		FindIterable<Document> docs = wxAservice.getWxArticleCollect().find(Filters.and(Filters.lte("updateTime",java.lang.Long.valueOf("1467763189886")),Filters.gte("updateTime",java.lang.Long.valueOf("1467763189884"))));

		new IGaiaImpl().getListBasicObject(docs);

	}
}
