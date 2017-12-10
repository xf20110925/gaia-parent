package com.ptb.gaia.search.article;

import com.ptb.gaia.service.entity.article.GArticleBasic;
import com.ptb.gaia.service.entity.article.GWbArticle;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * Created by watson zhang on 16/7/19.
 */
public interface IArticleSearch {
    public static String IArticleIndex = "article_m";
    public static String TWxArticleType = "wxArticle";
    public static String TWbArticleType = "wbArticle";

    enum AOrderType {
        E_BY_ADD_TIME
    }

    enum EPType {
        E_T_WX,
        E_T_WB,
    }

    <T extends GArticleBasic> List<T> search(
            String keyword, long time, Class<T> tClass, int start, int end);

    <T extends GArticleBasic> List<T> search(
            String keyword, Class<T> tClass, int start, int end);

    <T extends GArticleBasic> List<T> contentSearchByPmid(
            String keyword, Class<T> tClass, int start, int end);

    <T extends GArticleBasic> List<T> getArticleSearchByPmid(
            String keyword, Class<T> tClass, int start, int end);

    <T extends GArticleBasic> Pair<Long, List<T>> searchWbArticle(String keyword, int sortType, long time, Class<T> tClass, int start, int end);

    <T extends GArticleBasic> Pair<Long, List<T>> searchArticleSort(String keyword, int sortType, long time, Class<T> tClass, int start, int end);

    Pair<Long, List<GWbArticle>> searchWbTopic(String keyword, int start, int end, long postTime);

    <T extends GArticleBasic> Pair<Long, List<T>> searchArticleContent(String keyword, int sortType, Class<T> tClass, int start, int end);

    /**
     * @Description:根据WX WB Article url 返回 content List[]
     * @param:Set[url]
     * @return: List<Content>
     * @author: mengchen
     * @Date: 2017/2/14
     */
    <T extends GArticleBasic> List<String> searchArticleContentByUrl(Set<String> url,Class<T> tClass);

/*
    *//**
     * 搜索微信文章标题
     * @param keyword
     * @param sortType
     * @param start
     * @param end
     * @param <T>
     * @return
     *//*
    <T extends  GArticleBasic> Pair<Long, List<T>> searchWxArticleTitle(String keyword, int sortType, int start, int end);

    *//**
     * 搜索微博文章标题
     * @param keyword
     * @param sortType
     * @param start
     * @param end
     * @param <T>
     * @return
     *//*
    <T extends  GArticleBasic> Pair<Long, List<T>> searchWbArticleTitle(String keyword, int sortType, int start, int end);

    *//**
     * 搜索微信文章内容
     * @param keyword
     * @param sortType
     * @param start
     * @param end
     * @param <T>
     * @return
     *//*
    <T extends  GArticleBasic> Pair<Long, List<T>> searchWxArticleContent(String keyword, int sortType, int start, int end);

    *//**
     * 搜索微博文章内容
     * @param keyword
     * @param sortType
     * @param start
     * @param end
     * @param <T>
     * @return
     *//*
    <T extends  GArticleBasic> Pair<Long, List<T>> searchWbArticleContent(String keyword, int sortType, int start, int end);
    */
}
