package com.ptb.gaia.service.entity.media;

import com.ptb.gaia.service.entity.article.HotToken;
import com.ptb.gaia.service.entity.article.Point;

import java.io.Serializable;
import java.util.*;

import static java.util.Arrays.asList;

/**
 * Created by MengChen on 2016/10/11.
 */
public class WbMediaStatistics implements Serializable{


    public WbMediaStatistics() {

    }


    List<String> itags;

    public List<String> getItags() {
        return itags;
    }

    public void setItags(List<String> itags) {
        this.itags = itags;
    }

    /*
     */
    /*
     */
    /**
     * DefaultPeroidPoint constructor
     */
    public void reset() {
        this.setIsOriginal(0);
        this.setPubNumInPeroid(0);
        this.setPubOriginalArticleNumInPeroid(0);
        this.setAvgCommentNumInPeroid(0);
        this.setAvgForwardNumInPeroid(0);
        this.setAvgLikeNumInPeroid(0);
        this.setTotalCommentPoints(DefaultPeroidPoint());
        this.setTotalLikePoints(DefaultPeroidPoint());
        this.setTotalForwardPoints(DefaultPeroidPoint());
        this.setAvgLikeRateInPeroid(0.0);
        this.setAvgCommentRateInPeroid(0.0);
        this.setAvgForwardRateInPeroid(0.0);
        this.setAvgLikeRateRankInPeroid(-1);
        this.setAvgCommentRateRankInPeroid(-1);
        this.setAvgForwardRateRankInPeroid(-1);
    }


    /**
     *
     */
    private Integer isOriginal = -1;

    /**
     *
     */
    private Integer pubNumInPeroid = -1;

    /**
     *
     */
    private Integer pubOriginalArticleNumInPeroid = -1;

    /**
     *
     */
    private Integer avgCommentNumInPeroid = -1;

    /**
     *
     */
    private Integer avgForwardNumInPeroid = -1;

    /**
     *
     */
    private Integer avgLikeNumInPeroid = -1;

    Double avgLikeRateInPeroid = -1000000.0;
    Double avgCommentRateInPeroid = -1000000.0;
    Double avgForwardRateInPeroid = -1000000.0;
    Integer avgLikeRateRankInPeroid = -1;
    Integer avgCommentRateRankInPeroid = -1;
    Integer avgForwardRateRankInPeroid = -1;

    private List<HotToken> mediaHotWords = asList();


    public Double getAvgLikeRateInPeroid() {
        return avgLikeRateInPeroid;
    }

    public void setAvgLikeRateInPeroid(Double avgLikeRateInPeroid) {
        this.avgLikeRateInPeroid = avgLikeRateInPeroid;
    }

    public Double getAvgCommentRateInPeroid() {
        return avgCommentRateInPeroid;
    }

    public void setAvgCommentRateInPeroid(Double avgCommentRateInPeroid) {
        this.avgCommentRateInPeroid = avgCommentRateInPeroid;
    }

    public Double getAvgForwardRateInPeroid() {
        return avgForwardRateInPeroid;
    }

    public void setAvgForwardRateInPeroid(Double avgForwardRateInPeroid) {
        this.avgForwardRateInPeroid = avgForwardRateInPeroid;
    }

    public Integer getAvgLikeRateRankInPeroid() {
        return avgLikeRateRankInPeroid;
    }

    public void setAvgLikeRateRankInPeroid(Integer avgLikeRateRankInPeroid) {
        this.avgLikeRateRankInPeroid = avgLikeRateRankInPeroid;
    }

    public Integer getAvgCommentRateRankInPeroid() {
        return avgCommentRateRankInPeroid;
    }

    public void setAvgCommentRateRankInPeroid(Integer avgCommentRateRankInPeroid) {
        this.avgCommentRateRankInPeroid = avgCommentRateRankInPeroid;
    }

    public Integer getAvgForwardRateRankInPeroid() {
        return avgForwardRateRankInPeroid;
    }

    public void setAvgForwardRateRankInPeroid(Integer avgForwardRateRankInPeroid) {
        this.avgForwardRateRankInPeroid = avgForwardRateRankInPeroid;
    }

    private static List<Point> DefaultPeroidPoint() {
        Calendar instance = Calendar.getInstance(Locale.CHINESE);
        instance.add(Calendar.DATE, -1);
        List<Point> list = new LinkedList<>();

        for (int i = 0; i < 30; i++) {
            instance.add(Calendar.DATE, -1);
            list.add(new Point(instance.getTimeInMillis(), 0));
        }
        return list;
    }


    private List<Point> formatePoints(List<Point> headReadPoints) {
        Calendar instance = Calendar.getInstance(Locale.CHINESE);
        instance.set(Calendar.HOUR_OF_DAY, 0);
        instance.set(Calendar.MINUTE, 0);
        instance.set(Calendar.SECOND, 0);
        instance.set(Calendar.MILLISECOND, 0);
        instance.add(Calendar.DATE, -1);
        List<Point> list = new LinkedList<>();

        for (int i = 0; i < 30; i++) {
            instance.add(Calendar.DATE, -1);
            Optional<Point> first = headReadPoints.stream().filter(point -> {
                return point.getTime() >= instance.getTimeInMillis() && point.getTime() < (instance.getTimeInMillis() + 3600 * 24 * 1000);
            }).findFirst();
            if (first.isPresent()) {
                list.add(first.get());
            } else {
                list.add(new Point(instance.getTimeInMillis(), 0));
            }
        }
        return list;

    }


    /**
     *
     */
    private List<Point> totalCommentPoints = DefaultPeroidPoint();

    /**
     *
     */
    private List<Point> totalForwardPoints = DefaultPeroidPoint();
    ;

    /**
     *
     */
    private List<Point> totalLikePoints = DefaultPeroidPoint();

    public Integer getIsOriginal() {
        return isOriginal;
    }

    public void setIsOriginal(Integer isOriginal) {
        this.isOriginal = isOriginal;
    }

    public Integer getPubNumInPeroid() {
        return pubNumInPeroid;
    }

    public void setPubNumInPeroid(Integer pubNumInPeroid) {
        this.pubNumInPeroid = pubNumInPeroid;
    }

    public Integer getPubOriginalArticleNumInPeroid() {
        return pubOriginalArticleNumInPeroid;
    }

    public void setPubOriginalArticleNumInPeroid(Integer pubOriginalArticleNumInPeroid) {
        this.pubOriginalArticleNumInPeroid = pubOriginalArticleNumInPeroid;
        this.isOriginal = this.pubOriginalArticleNumInPeroid > 0 ? 1 : 0;
    }

    public Integer getAvgCommentNumInPeroid() {
        return avgCommentNumInPeroid;
    }

    public void setAvgCommentNumInPeroid(Integer avgCommentNumInPeroid) {
        this.avgCommentNumInPeroid = avgCommentNumInPeroid;
    }

    public Integer getAvgForwardNumInPeroid() {
        return avgForwardNumInPeroid;
    }

    public void setAvgForwardNumInPeroid(Integer avgForwardNumInPeroid) {
        this.avgForwardNumInPeroid = avgForwardNumInPeroid;
    }

    public Integer getAvgLikeNumInPeroid() {
        return avgLikeNumInPeroid;
    }

    public void setAvgLikeNumInPeroid(Integer avgLikeNumInPeroid) {
        this.avgLikeNumInPeroid = avgLikeNumInPeroid;
    }

    public List<Point> getTotalCommentPoints() {
        return totalCommentPoints;
    }

    public void setTotalCommentPoints(List<Point> totalCommentPoints) {
        this.totalCommentPoints = formatePoints(totalCommentPoints);
    }

    public List<Point> getTotalForwardPoints() {
        return totalForwardPoints;
    }

    public void setTotalForwardPoints(List<Point> totalForwardPoints) {
        this.totalForwardPoints = formatePoints(totalForwardPoints);
    }

    public List<Point> getTotalLikePoints() {
        return totalLikePoints;
    }

    public void setTotalLikePoints(List<Point> totalLikePoints) {
        this.totalLikePoints = formatePoints(totalLikePoints);
    }

    public List<HotToken> getMediaHotWords() {
        return mediaHotWords;
    }

    public void setMediaHotWords(List<HotToken> mediaHotWords) {
        this.mediaHotWords = mediaHotWords;
    }

}
