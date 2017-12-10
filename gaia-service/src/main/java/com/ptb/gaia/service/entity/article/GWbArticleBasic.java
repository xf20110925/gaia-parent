package com.ptb.gaia.service.entity.article;

/**
 *
 */
public class GWbArticleBasic extends GArticleBasic {

    /**
     * DefaultPeroidPoint constructor
     */
    public GWbArticleBasic() {
    }

    /**
     *
     */
    private String articleType;

    /**
     *
     */
    private String source;

    /**
     *
     */
    private int commentNum=-1;

    /**
     *
     */
    private int forwardNum = -1;


    private int zpzNum = -1;

    private Integer isOriginal;

    /**
     *
     */
    private Integer likeNum = -1;

    public Integer getIsOriginal() {
        return isOriginal;
    }

    public void setIsOriginal(Integer isOrginal) {
        this.isOriginal = isOrginal;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Integer getCommentNum() {
        return commentNum;
    }

    public void setCommentNum(Integer commentNum) {
        if(commentNum != null) {
            this.commentNum = commentNum;
        }
    }

    public Integer getForwardNum() {
        return forwardNum;
    }

    public void setForwardNum(Integer forwardNum) {
        if(forwardNum != null) {
            this.forwardNum = forwardNum;
        }
    }

    public Integer getLikeNum() {
        return likeNum;
    }

    public Integer getZpzNum() {
        return zpzNum;
    }

    public void setZpzNum(Integer zpzNum) {
        if(zpzNum != null) {
            this.zpzNum = zpzNum;
        }
    }

    public String getArticleType() {
        return articleType;
    }

    public void setArticleType(String articleType) {
        this.articleType = articleType;
    }

    public void setLikeNum(Integer likeNum) {
        if(likeNum != null) {
            this.likeNum = likeNum;
        }
    }
}