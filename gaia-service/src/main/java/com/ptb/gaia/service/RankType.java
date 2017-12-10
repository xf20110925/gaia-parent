package com.ptb.gaia.service;

public enum RankType {
    RANK_HEAD_VERG_READ("avgHeadReadNumInPeroid"),  //头条平均阅读数
    RANK_HEAD_VERG_LIKE("avgHeadLikeNumInPeroid"),  //头条平均点赞数

    RANK_HEAD_VERG_READ_RATE("avgHeadReadRateRankInPeroid"),  //头条平均阅读增长率
    RANK_HEAD_VERG_LIKE_RATE("avgHeadLikeRateRankInPeroid"),  //头条平均阅读增长率

    RANK_VERG_COMMENT("avgCommentNumInPeroid"),  //平均评论数
    RANK_VERG_COMMENT_RATE_RANK("avgCommentRateRankInPeroid"),  //平均评论增长率排名

    RANK_VERG_LIKE("avgLikeNumInPeroid"),  //平均点赞数
    RANK_VERG_LIKE_RATE_RANK("avgLikeRateRankInPeroid"),  //平均点赞数增长率排名

    RANK_VERG_FORWARD("avgForwardNumInPeroid"),  //平均转发数
    RANK_VERG_FORWARD_RATE_RANK("avgForwardRateRankInPeroid")  //转发增长率排名
    ;


    private String value;

    RankType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}