package com.ptb.gaia.service.entity.article;

/**
 *
 */
public class ArticleRank {

    /**
     * DefaultPeroidPoint constructor
     */
    public ArticleRank() {
    }

    public ArticleRank(String articleID, Long score) {
        this.articleID = articleID;
        this.score = score;
    }

    /**
     *
     */
    private String articleID;

    /**
     *
     */
    private Long score;

    public String getArticleID() {
        return articleID;
    }

    public void setArticleID(String articleID) {
        this.articleID = articleID;
    }

    public Long getScore() {
        return score;
    }

    public void setScore(Long score) {
        this.score = score;
    }
}