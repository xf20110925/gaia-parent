package com.ptb.gaia.service.entity.article;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by eric on 16/6/24.
 */
public class GArticleSet<T extends GArticleBasic> {
    public static final GArticleSet Null = new GArticleSet(0, new ArrayList());
    int total;
    List<T> articles;

    public GArticleSet() {
    }

    public GArticleSet(int i, List<T> arrayList) {
        this.total = i;
        this.articles = arrayList;

    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public List<T> getArticles() {
        return articles;
    }

    public void setArticles(List<T> articles) {
        this.articles = articles;
    }
}
