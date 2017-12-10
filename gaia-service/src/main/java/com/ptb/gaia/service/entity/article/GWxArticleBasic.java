package com.ptb.gaia.service.entity.article;

/**
 *
 */
public class GWxArticleBasic extends GArticleBasic implements Cloneable {

    /**
     * DefaultPeroidPoint constructor
     */
    public GWxArticleBasic() {
    }

    /**
     *
     */
    private Integer isOriginal;

    /**
     *
     */
    private int likeNum = -1;

    /**
     *
     */
    private int readNum = -1;

    /**
     *article position: idx
     */
    private Integer position;

    /**
     *
     */
    private String author;

    private String filePath;

    public Integer getIsOriginal() {
        return isOriginal;
    }

    public void setIsOriginal(Integer isOriginal) {
        this.isOriginal = isOriginal;
    }

    public Integer getLikeNum() {
        return likeNum;
    }

    public void setLikeNum(Integer likeNum) {
        if(likeNum != null) {
            this.likeNum = likeNum;
        }
    }

    public Integer getReadNum() {
        return readNum;
    }

    public void setReadNum(Integer readNum) {
        if(readNum != null) {
            this.readNum = readNum;
        }
    }

    public Integer getPosition() {
        return position;
    }

    public void setPosition(Integer position) {
        this.position = position;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public Object clone() {
        GWxArticleBasic wx = null;
        try{
            wx = (GWxArticleBasic)super.clone();
        }catch(CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return wx;
    }


}