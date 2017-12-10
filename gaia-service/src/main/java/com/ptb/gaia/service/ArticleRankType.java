package com.ptb.gaia.service;

import com.ptb.gaia.service.entity.article.GWbArticle;
import com.ptb.gaia.service.entity.article.GWxArticle;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public enum ArticleRankType {
    RANK_POSTTIME_WX(1, Arrays.asList("postTime"), Comparator.comparingLong(GWxArticle::getPostTime).thenComparingLong(GWxArticle::getLikeNum).reversed()),
    RANK_POSTTIME_WB(2, Arrays.asList("postTime"), Comparator.comparingLong(GWbArticle::getPostTime).thenComparingLong(GWbArticle::getLikeNum).reversed()),
    RANK_INVOLVE_NUM_WX(3, Arrays.asList("readNum", "likeNum"), Comparator.comparingInt(GWxArticle::getReadNum).thenComparingInt(GWxArticle::getLikeNum).reversed()),
    RANK_INVOLVE_NUM_WB(4, Arrays.asList("zpzNum"), Comparator.comparingInt(GWbArticle::getZpzNum).thenComparingLong(GWbArticle::getUpdateTime).reversed());

    private int code;
    private List<String> sortFields;
    private Comparator comparator;

    ArticleRankType(int code, List<String> sortFields, Comparator comparator) {
        this.code = code;
        this.sortFields = sortFields;
        this.comparator = comparator;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public List<String> getSortFields() {
        return sortFields;
    }

    public void setSortFields(List<String> sortFields) {
        this.sortFields = sortFields;
    }

    public Comparator getComparator() {
        return comparator;
    }

    public void setComparator(Comparator comparator) {
        this.comparator = comparator;
    }
}