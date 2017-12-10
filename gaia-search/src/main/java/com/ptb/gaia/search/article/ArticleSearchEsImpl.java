package com.ptb.gaia.search.article;

import com.alibaba.fastjson.JSON;
import com.ptb.gaia.service.entity.article.GArticleBasic;
import com.ptb.gaia.service.entity.article.GWbArticle;
import com.ptb.gaia.service.entity.article.GWxArticle;

import com.ptb.gaia.utils.EsClient;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.seleniumhq.jetty7.util.security.Credential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by eric on 16/8/8.
 */
public class ArticleSearchEsImpl implements IArticleSearch {
    public static String TWxArticleType = "wxArticle";
    public static String TWbArticleType = "wbArticle";
    Logger logger = LoggerFactory.getLogger(ArticleSearchEsImpl.class);
    TransportClient cli;
    static Configuration conf;
    private static String articleIndex;
    private static String articleContentIndex;

    public ArticleSearchEsImpl() {
        try {
            conf = new PropertiesConfiguration("ptb.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        articleIndex = conf.getString("gaia.es.article.title.index", "article_m");
        articleContentIndex = conf.getString("gaia.es.article.content.index", "article_content");
        cli = EsClient.getCli();

    }

    public <T extends GArticleBasic> Pair<Long, List<T>> searchArticleTitle(
            Class<T> tClass, int start, int end, String articleType, QueryBuilder query,
            SortBuilder sortBuilder, QueryBuilder filters, String source) {

        return this.searchArticle(tClass, start, end, articleIndex, articleType, query, sortBuilder, filters, source);
    }

    public <T extends GArticleBasic> Pair<Long, List<T>> searchArticleContent(
            Class<T> tClass, int start, int end, String articleType, QueryBuilder query,
            SortBuilder sortBuilder, QueryBuilder filters, String source) {

        return this.searchArticle(tClass, start, end, articleContentIndex, articleType, query, sortBuilder, filters, source);
    }

    public <T extends GArticleBasic> Pair<Long, List<T>> searchArticle(
            Class<T> tClass,
            int start,
            int end,
            String indexName,
            String articleType,
            QueryBuilder query,
            SortBuilder sortBuilder,
            QueryBuilder filters,
            String source) {

        SearchRequestBuilder searchRequestBuilder = cli.prepareSearch(indexName)
                .setTypes(articleType)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setFrom(start)
                .setSize(end - start)
                .setQuery(query)
                //.setExplain(true)
                .setTrackScores(true);

        if (source != null) {
            searchRequestBuilder.setSource(source);
        }

        if (sortBuilder != null) {
            searchRequestBuilder.addSort(sortBuilder);
            searchRequestBuilder.addSort(SortBuilders.scoreSort());
        }

        if (filters != null) {
            searchRequestBuilder.setPostFilter(filters);
        }

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        searchResponse.getHits().getTotalHits();
        List<T> ret = new LinkedList<T>();
        for (SearchHit searchHitFields : searchResponse.getHits().getHits()) {
            ret.add(JSON.parseObject(searchHitFields.getSourceAsString(), tClass));
        }
        //MediaSearchUtil.print(searchResponse);
        long totalNum;
        if (searchResponse.getHits().getTotalHits() >= 9900) {//es限制from+size不能大于10000，所以设置返回的总数最大就9900。
            totalNum = 9900;
        } else {
            totalNum = searchResponse.getHits().getTotalHits();
        }
        return new ImmutablePair(totalNum, ret);
    }

    @Override
    public <T extends GArticleBasic> List<T> search(
            String keyword, long time, Class<T> tClass, int start, int end) {
        String type = "";
        QueryBuilder query = null;
        QueryBuilder filter = null;
        if (GWxArticle.class == tClass) {
            type = TWxArticleType;
            query = QueryBuilders.matchQuery("title", keyword);
        }
        if (GWbArticle.class == tClass) {
            query = QueryBuilders.matchQuery("title", keyword);
            type = TWbArticleType;
        }
        return searchArticleTitle(tClass, start, end, type, query, null, filter, null).getRight();
    }

    @Override
    public <T extends GArticleBasic> List<T> search(
            String keyword, Class<T> tClass, int start, int end) {
        return search(keyword, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7), tClass, start, end);
    }

    @Override
    public <T extends GArticleBasic> List<T> contentSearchByPmid(
            String pmid, Class<T> tClass, int start, int end) {
        return getArticleSearchByPmid(pmid, tClass, start, end);
    }

    @Override
    public <T extends GArticleBasic> List<T> getArticleSearchByPmid(
            String pmid, Class<T> tClass, int start, int end) {

        String type = "";
        QueryBuilder query = null;

        if (GWxArticle.class == tClass) {
            type = TWxArticleType;
            query = QueryBuilders.multiMatchQuery(pmid, "pmid");
        }

        return searchArticleTitle(tClass, start, end, type, query, null, null, null).getRight();
    }

    @Override
    public <T extends GArticleBasic> Pair<Long, List<T>> searchWbArticle(String keyword, int sortType, long time, Class<T> tClass, int start, int end) {
        Pair<Long, List<T>> exactListPair = this.searchArticleExact(keyword, tClass, start, end);
        return exactListPair;
        //Pair<Long, List<T>> sortListPair = this.searchArticleSort(keyword, sortType, time, tClass, start, end);
        //if (exactListPair.getRight().size() <= (end - start)) {
        //    return new ImmutablePair(exactListPair.getLeft() + sortListPair.getLeft(), exactListPair.getRight());
        //}
        //Pair<Long, List<T>> lastListPair = this.mergeSearchResult(sortListPair, exactListPair);
        //return lastListPair;
    }

    @Override
    public <T extends GArticleBasic> Pair<Long, List<T>> searchArticleSort(String keyword, int sortType, long time, Class<T> tClass, int start, int end) {
        String type = "";
        QueryBuilder filter = null;
        if (GWxArticle.class == tClass) {
            type = TWxArticleType;
        }
        if (GWbArticle.class == tClass) {
            type = TWbArticleType;
        }
        QueryBuilder query = QueryBuilders.matchQuery("title", keyword);
        MatchQueryBuilder queryNot = QueryBuilders.matchPhraseQuery("title", keyword);
        BoolQueryBuilder should = QueryBuilders.boolQuery().mustNot(queryNot).must(query);
        Pair<Long, List<T>> listPair = searchArticleTitle(tClass, start, end, type, should, this.getSortBuilder(sortType), filter, null);
        return listPair;
    }


    public <T extends GArticleBasic> Pair<Long, List<T>> searchArticleExact(String keyword, Class<T> tClass, int start, int end) {
        String type = "";
        QueryBuilder filter = null;
        if (GWxArticle.class == tClass) {
            type = TWxArticleType;
        }
        if (GWbArticle.class == tClass) {
            type = TWbArticleType;
        }
        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("title", keyword);
        Pair<Long, List<T>> listPairExact = searchArticleTitle(tClass, start, end, type, queryBuilder, null, filter, null);
        return listPairExact;
    }

    public <T extends GArticleBasic> Pair<Long, List<T>> mergeSearchResult(Pair<Long, List<T>> sortListPair, Pair<Long, List<T>> exactListPair) {
        if (sortListPair == null || exactListPair == null) {
            return null;
        }
        if (sortListPair.getRight().size() != 0) {
            int index = sortListPair.getRight().size() - 1;
            int listSize = 0;
            for (; listSize < exactListPair.getRight().size(); listSize++, index--) {
                sortListPair.getRight().remove(index);
            }
        }
        sortListPair.getRight().addAll(0, exactListPair.getRight());
        return sortListPair;
    }

    private SortBuilder getSortBuilder(int sortType) {
        SortBuilder sortBuilder;
        if (sortType == 7) {
            //Script script = new Script("return doc['relativity'].value*_score");
            //sortBuilder = SortBuilders.scriptSort(script, "number");
            sortBuilder = null;
        } else if (sortType == 8) {
            sortBuilder = null;
        } else {
            logger.error("sortType is error! {}", sortType);
            return null;
        }
        return sortBuilder;
    }

    @Override
    public Pair<Long, List<GWbArticle>> searchWbTopic(String keyword, int start, int end, long postTime) {
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.queryStringQuery(keyword).defaultField("content").minimumShouldMatch("100%").analyzer("ik"));
        try {
            String source = XContentFactory.jsonBuilder().startObject().array("excludes", "dynamics", "hotToken").string();
            FieldSortBuilder zpzNum = SortBuilders.fieldSort("zpzNum").order(SortOrder.DESC);
            return searchArticleTitle(GWbArticle.class, start, end, TWbArticleType, query, zpzNum, null, source);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ImmutablePair.of(0L, Collections.emptyList());
    }

    @Override
    public <T extends GArticleBasic> Pair<Long, List<T>> searchArticleContent(String keyword, int sortType, Class<T> tClass, int start, int end) {
        MatchQueryBuilder content = QueryBuilders.matchQuery("content", keyword);
        SortBuilder contentSortBuilder = null;

        String type = null;
        if (tClass == GWxArticle.class) {
            type = TWxArticleType;
        } else if (tClass == GWbArticle.class) {
            type = TWbArticleType;
        }
        Pair<Long, List<T>> listPairExact = searchArticleContent(tClass, start, end, type, content, contentSortBuilder, null, null);
        return listPairExact;
    }

    @Override
    public <T extends GArticleBasic> List<String> searchArticleContentByUrl(Set<String> urls, Class<T> tClass) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        urls.stream().map(url -> Credential.MD5.digest(url).substring(4)).forEach(urlMd5 -> {
            if (tClass == GWxArticle.class) {
                multiGetRequest.add(articleContentIndex, TWxArticleType, urlMd5);
            } else if (tClass == GWbArticle.class) {
                multiGetRequest.add(articleContentIndex, TWbArticleType, urlMd5);
            }
        });
        List<String> articles = new ArrayList<>();

        MultiGetResponse multiGetItemResponses = cli.multiGet(multiGetRequest).actionGet();
        multiGetItemResponses.forEach(multiGetItemResponse -> {
            Map<String, Object> source = multiGetItemResponse.getResponse().getSource();
            if (source != null && source.get("articleUrl") != null && source.get("content") != null && source.get("content").toString().length() > 200) {
                articles.add(String.format("%s:::%s", source.get("articleUrl"), source.get("content")));
            }
        });

        return articles;
    }

    //测试用
    public Pair<Long, List<GWbArticle>> searchWbArticle(List<String> urlList, int start, int end) {
        try {
            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().should(QueryBuilders.termQuery("articleUrl", ""));
            urlList.forEach(m -> {
                queryBuilder.should(QueryBuilders.matchQuery("articleUrl", m));
            });
            SortBuilder sortBuilder = SortBuilders.fieldSort("timeStamp");
            Pair<Long, List<GWbArticle>> search = searchArticleTitle(GWbArticle.class, start, end - start, TWbArticleType, queryBuilder, null, null, null);
            return null;
        } catch (Exception e) {
            //logger.error("mediaName: {}, startPos: {}, endPos: {}", mediaName, startPos, endPos);
            return null;
        }
    }

    public static void main(String[] args) {

        ArticleSearchEsImpl articleSearchEs = new ArticleSearchEsImpl();

        String index = "article_content";
        String type = "wxArticle";
        List<String> idList = new ArrayList<>();
        idList.add("1ed0a3579c24da26bdc1c49369ba62b2");
        idList.add("5f7ae3f2da78f1d6a0adccf275ec555f");
        idList.add("58b3513a2779d592c004c7b1ca722906");
        idList.add("a37c611ac9476cb6be6de25c072df407");
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        idList.forEach(id -> {
            multiGetRequest.add(index, type, id);
        });

        MultiGetResponse multiGetItemResponses = articleSearchEs.cli.multiGet(multiGetRequest).actionGet();
        List<String> articles = new ArrayList<>();
        multiGetItemResponses.forEach(multiGetItemResponse -> {
            articles.add(String.format("%s:::%s",
                    multiGetItemResponse.getResponse().getSource().get("articleUrl"),
                    multiGetItemResponse.getResponse().getSource().get("content")));
        });
        System.out.println(articles);
    }
}
