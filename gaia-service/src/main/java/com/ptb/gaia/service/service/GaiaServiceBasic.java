package com.ptb.gaia.service.service;

import com.alibaba.fastjson.JSON;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.ptb.gaia.service.RankType;
import com.ptb.gaia.service.entity.article.GArticleBasic;
import com.ptb.gaia.service.entity.article.GWxArticle;
import com.ptb.gaia.service.entity.media.GMediaBasic;
import com.ptb.gaia.service.entity.media.GMediaSet;
import com.ptb.gaia.service.utils.JedisUtil;
import com.ptb.gaia.service.utils.MongoUtils;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


/**
 * Created by eric on 16/6/23.
 */
public abstract class GaiaServiceBasic {
	static Logger logger = LoggerFactory.getLogger(GaiaServiceBasic.class);
	private final String mongodbHost = null;
	private final String mongodbHostArray[];
	private final int mongodbPort;
//	private String gaiaDbName = "gaiaV2";
	private String gaiaDbName = "test";
	public MongoClient mongoClient;
	public MongoClient primaryMongoClient;

//	//分片
//	public MongoClient mongoClient1;
//	private final String mongodbHost1;
//	private final int mongodbPort1;

	protected final UpdateOptions updateOptions = new UpdateOptions().upsert(true);
	protected final int cacheExpireSecond = 600;
	public static int MaxHotMediaNum = 100;
	public static int MaxCapacityMediaNum = 50;
	public static int HOT_ARTICLE_BATCH_SIZE = 100;
	public static int RECENT_ARTICLE_BATCH_SIZE = 100;
	protected static final long NINE_DAY_TIME = 9 * 24 * 3600 * 1000;
	protected static final long SEVEN_DAY_TIME = 7 * 24 * 3600 * 1000;
	protected static final long TWO_DAY_TIME = 2 * 24 * 3600 * 1000;
	protected final UpdateOptions updateFalseOptions = new UpdateOptions().upsert(false);
	Configuration conf;

	public GaiaServiceBasic() throws ConfigurationException {
		conf = new PropertiesConfiguration("ptb.properties");
		//mongodbHost = conf.getString("gaia.service.mongodb.host");
		mongodbHostArray = conf.getStringArray("gaia.service.mongodb.host");
		mongodbPort = conf.getInt("gaia.service.mongodb.port");
		gaiaDbName = conf.getString("gaia.service.mongodb.db");
		MaxHotMediaNum = conf.getInt("gaia.service.media.hot.max.num", 100);
		MaxCapacityMediaNum = conf.getInt("gaia.service.media.capacity.max.num", 50);
		HOT_ARTICLE_BATCH_SIZE = conf.getInt("gaia.service.article.hot.max.num", 100);
		RECENT_ARTICLE_BATCH_SIZE = conf.getInt("gaia.service.article.recent.max.num", 100);

		//MongoUtils.configure(mongodbHost, mongodbPort);
		//分片
//		mongodbHost1 = conf.getString("gaia.service.mongodb.host1");
//		mongodbPort1 = conf.getInt("gaia.service.mongodb.port1");
//		MongoUtils1.configure(mongodbHost1, mongodbPort1);
//		mongoClient1 = MongoUtils1.instance();

		MongoUtils.configure(mongodbHostArray, mongodbPort);
		mongoClient = MongoUtils.instance();

		MongoUtils.configure(mongodbHostArray, mongodbPort);
		primaryMongoClient = MongoUtils.primaryInstance();

	}

	//	//分片
//	public MongoCollection<Document> getWxMediaCollectfenpian() {
//		return mongoClient1.getDatabase(gaiaDbName).getCollection("wxMedia");
//	}
//
//	//分片
//	public MongoCollection<Document> getWxArticleCollectfenpian() {
//		return mongoClient1.getDatabase(gaiaDbName).getCollection("wxArticle");
//	}

	//写操作 用主机

	public MongoCollection<Document> getCollectByPrimary(String collection) {
		return primaryMongoClient.getDatabase(gaiaDbName).getCollection(collection);
	}

	public MongoCollection<Document> getWbMediaCollectByPrimary() {
		return primaryMongoClient.getDatabase(gaiaDbName).getCollection("wbMedia");
	}

	public MongoCollection<Document> getRecommandCollectByPrimary() {
		return primaryMongoClient.getDatabase(gaiaDbName).getCollection("machineRecommandCategoryMedia");
	}

	public MongoCollection<Document> getWxMediaCollectByPrimary() {
		return primaryMongoClient.getDatabase(gaiaDbName).getCollection("wxMedia");
	}

	public MongoCollection<Document> getWxArticleCollectByPrimary() {
		return primaryMongoClient.getDatabase(gaiaDbName).getCollection("wxArticle");
	}

	public MongoCollection<Document> getWbArticleCollectByPrimary() {
		return primaryMongoClient.getDatabase(gaiaDbName).getCollection("wbArticle");
	}

	public MongoCollection<Document> getLiveMediaCollectByPrimary(){
		return primaryMongoClient.getDatabase(gaiaDbName).getCollection("liveMedia");
	}

	public MongoCollection<Document> getLiveArticleCollectByPrimary(){
		return primaryMongoClient.getDatabase(gaiaDbName).getCollection("liveCase");
	}

	public MongoCollection<Document> getVedioCollectByPrimary(){
		return primaryMongoClient.getDatabase(gaiaDbName).getCollection("vedioMedia");
	}

	//读取用从节点

	public MongoCollection<Document> getCollect(String collection) {
		return mongoClient.getDatabase(gaiaDbName).getCollection(collection);
	}

	public MongoCollection<Document> getWbMediaCollect() {
		return mongoClient.getDatabase(gaiaDbName).getCollection("wbMedia");
	}

	public MongoCollection<Document> getRecommandCollect() {
		return mongoClient.getDatabase(gaiaDbName).getCollection("machineRecommandCategoryMedia");
	}

	public MongoCollection<Document> getWxMediaCollect() {
		return mongoClient.getDatabase(gaiaDbName).getCollection("wxMedia");
	}

	public MongoCollection<Document> getWxArticleCollect() {
		return mongoClient.getDatabase(gaiaDbName).getCollection("wxArticle");
	}

	public MongoCollection<Document> getWbArticleCollect() {
		return mongoClient.getDatabase(gaiaDbName).getCollection("wbArticle");
	}

	public MongoCollection<Document> getLiveMediaCollect() {
		return mongoClient.getDatabase(gaiaDbName).getCollection("liveMedia");
	}

	public MongoCollection<Document> getCollectByName(String collecName) {
		return mongoClient.getDatabase(gaiaDbName).getCollection(collecName);
	}



	protected String generateCachePmidRankKey(String prefix, List<String> pmids) {
		String key = pmids.stream().filter(StringUtils::isNotBlank).sorted(
			(k1, k2) -> k1.compareTo(k2)
		).reduce("", (acc, ele) -> acc + ele);
		key = JedisUtil.getMD5Key(key);
		key = prefix + key;
		return key;
	}

	protected <T> Set<T> convertDocsToEntity(FindIterable<Document> docs, Class<T> clazz) {
		Set<T> result = new HashSet<>();
		for (Document doc : docs) {
		    doc.remove("fansData");
			try {
				T t = JSON.parseObject(JSON.toJSONString(doc), clazz);
				result.add(t);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	protected <T extends GMediaBasic> GMediaSet<T> getMediaRank(List<String> pmids, int start, int end, RankType rankType, int maxNum, IProcess iProcess,
	                                                            String cacheKeyPrefix, MongoCollection collection, Class<T> t) {
		//从缓存中拿数据
		if (start < 0 || start >= end || end == 0 || pmids == null) {
			return GMediaSet.NULL;
		}

		String cacheKey = generateCachePmidRankKey(String.format("%s:%s:", cacheKeyPrefix, rankType.name()), pmids);
		long totalPmidNum = JedisUtil.zKeySize(cacheKey);
		List<String> pmidsRank;


		if (totalPmidNum == 0) {
			List<String> rankpmid = iProcess.readAndFilterPmidFromDB(pmids, rankType, maxNum);

			cacheMedia(generateCachePmidRankKey(String.format("%s:%s:", cacheKeyPrefix, rankType.name()), pmids), rankpmid);
			pmidsRank = rankpmid.stream().
				skip(start).limit(end - start).
				collect(Collectors.toList());
			totalPmidNum = rankpmid.size();
		} else {
			pmidsRank = JedisUtil.zget(cacheKey, start, end - 1).stream().collect(Collectors.toList());
		}

		List<T> medias = getMedias(collection, pmidsRank, t);
		return new GMediaSet<>(Math.toIntExact(totalPmidNum), medias);
	}

	protected <T extends GMediaBasic> List<T> getMedias(MongoCollection<Document> mongoCollection, List<String> pmids, Class<T> tClass) {
		if (pmids == null || pmids.size() == 0) {
			return new ArrayList();
		}
		FindIterable<Document> Retdoc = mongoCollection.find(Filters.in("_id", pmids));
		Set<T> ts = convertDocsToEntity(Retdoc, tClass);

		Map<String, T> inNactiveMedia = ts.stream().map(media -> {
			if (media.getLatestArticle() == null) {
				return media;
			}
			media.reset();
			return media;
		}).collect(Collectors.toMap(T::getPmid, (k) -> k));


		return pmids.stream().map(pmid -> {
			T t = inNactiveMedia.get(pmid);
			if (t == null) {
				logger.error(String.format("missing media pmids [%s]", pmid));
			}
			return t;
		}).filter(k -> (k != null)).collect(Collectors.toList());

	}

	public <T extends GArticleBasic> List<T> getArticle(List<String> articleUrls, Class<T> tClass) {
		MongoCollection<Document> articleCollect;
		if (tClass == GWxArticle.class) {
			articleCollect = getWxArticleCollect();
		} else {
			articleCollect = getWbArticleCollect();
		}

		FindIterable<Document> docs = articleCollect.find(Filters.in("_id", articleUrls));
		Map<String, T> collect = convertDocsToEntity(docs, tClass).stream().collect(Collectors.toMap(T::getArticleUrl, (k) -> k));

		return articleUrls.stream().map(articleUrl -> {
			T t = collect.get(articleUrl);
			if (t == null) {
				logger.error(String.format("missing media pmids [%s]", articleUrl));
			}
			return t;
		}).filter(k -> (k != null)).collect(Collectors.toList());
	}


	public Date getTimeAfterTondayMiddleNight(int days) {
		Calendar instance = Calendar.getInstance(Locale.CHINESE);
		instance.set(Calendar.HOUR_OF_DAY, 0);
		instance.set(Calendar.MINUTE, 0);
		instance.set(Calendar.SECOND, 0);
		instance.set(Calendar.MILLISECOND, 0);
		instance.add(Calendar.DATE, days);
		return instance.getTime();
	}

	protected List<String> getMediaHotRankFromMongoDB(MongoCollection<Document> collect, List<String> pmids, RankType rankType, int maxNum) {
		List<String> sortedPmids = new LinkedList<>();
		Bson likeRateFilter = Filters.gt("avgLikeRateInPeroid", -1000000);
		Bson forwardRateFilter = Filters.gt("avgForwardRateInPeroid", -1000000);
		Bson commonRateFilter = Filters.gt("avgCommentRateInPeroid", -1000000);
		Bson headReadRateFilter = Filters.gt("avgHeadLikeRateInPeroid", -1000000);
		Bson headLikeRateFilter = Filters.gt("avgHeadReadRateInPeroid", -1000000);
		Bson filter = null;
		Bson sorter;


		switch (rankType) {
			case RANK_VERG_LIKE_RATE_RANK:
				filter = likeRateFilter;
				sorter = Sorts.ascending(rankType.getValue());
				break;
			case RANK_VERG_FORWARD_RATE_RANK:
				filter = forwardRateFilter;
				sorter = Sorts.ascending(rankType.getValue());
				break;
			case RANK_VERG_COMMENT_RATE_RANK:
				filter = commonRateFilter;
				sorter = Sorts.ascending(rankType.getValue());
				break;
			case RANK_HEAD_VERG_LIKE_RATE:
				filter = headReadRateFilter;
				sorter = Sorts.ascending(rankType.getValue());
				break;
			case RANK_HEAD_VERG_READ_RATE:
				filter = headLikeRateFilter;
				sorter = Sorts.ascending(rankType.getValue());
				break;
			case RANK_HEAD_VERG_READ:
			case RANK_VERG_FORWARD:
			case RANK_VERG_LIKE:
			case RANK_VERG_COMMENT:
				filter = Filters.gt(rankType.getValue(), 100);
				sorter = Sorts.descending(rankType.getValue());
				break;
			case RANK_HEAD_VERG_LIKE:
				filter = Filters.gt(rankType.getValue(), 0);
				sorter = Sorts.descending(rankType.getValue());
				break;
			default:
				sorter = Sorts.descending(rankType.getValue());
		}

		Bson filters = null;
		if (filter == null) {
			filters = Filters.and(Filters.in("_id", pmids), Filters.gte("updateTime", getTimeAfterTondayMiddleNight(-1).getTime())
				, Filters.gte("latestArticle.time", String.valueOf(getTimeAfterTondayMiddleNight(-8).getTime())));
		} else {
			filters = Filters.and(Filters.in("_id", pmids), Filters.gte("updateTime", getTimeAfterTondayMiddleNight(-1).getTime())
				, Filters.gte("latestArticle.time", String.valueOf(getTimeAfterTondayMiddleNight(-8).getTime())), filter);
		}
		FindIterable<Document> Retdoc = collect.find(filters).
			sort(sorter).limit(maxNum);

		for (Document doc : Retdoc)
			sortedPmids.add(doc.getString("_id"));

		return sortedPmids;
	}

	protected void cacheMedia(String key, List<String> pmids) {
		double rank = pmids.size() - 1;
		HashMap<String, Double> map = new HashMap<>();
		for (String pmid : pmids) {
			map.put(pmid, rank--);
		}
		JedisUtil.zadd(key, map, cacheExpireSecond);
	}

	public static void main(String[] args) throws ConfigurationException {
		System.out.println(String.valueOf(System.currentTimeMillis()));
	}

}
