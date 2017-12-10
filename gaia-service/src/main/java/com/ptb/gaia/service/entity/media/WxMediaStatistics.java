package com.ptb.gaia.service.entity.media;

import com.ptb.gaia.service.entity.article.HotToken;
import com.ptb.gaia.service.entity.article.Point;

import java.io.Serializable;
import java.util.*;

import static java.util.Arrays.asList;

/**
 * Created by MengChen on 2016/10/11.
 */
public class WxMediaStatistics implements Serializable {

    public void reset() {
        this.setIsOriginal(0);
        this.setTotalLikeNumInPeroid(0);
        this.setPubNumInPeroid(0);

        this.setAvgHeadLikeNumInPeroid(0);
        this.setAvgHeadReadNumInPeroid(0);
        this.setPubArticleNumInPeroid(0);
        this.setHotArticleNumInPeroid(0);
        this.setTotalLikeNumInPeroid(0);


        this.setAvgHeadReadRateInPeroid(0.0);
        this.setAvgHeadLikeRateInPeroid(0.0);

        this.setAvgHeadLikeRateRankInPeroid(-1);
        this.setAvgHeadReadRateRankInPeroid(-1);

        this.setThirdReadPoints(DefaultPeroidPoint());
        this.setSecondReadPoints(DefaultPeroidPoint());
        this.setHeadReadPoints(DefaultPeroidPoint());

    }

    /**
     * DefaultPeroidPoint constructor
     */
    public WxMediaStatistics() {
    }

    /**
     *
     */
    private Integer isOriginal = 0;

    /**
     *
     */
    private Integer pubOriginalArticleNumInPeroid = -1;

    /**
     *
     */
    private Integer avgHeadReadNumInPeroid = -1;

    /**
     *
     */
    private Integer avgHeadLikeNumInPeroid = -1;

    /**
     *
     */
    private Integer pubNumInPeroid = -1;

    /**
     *
     */
    private Integer pubArticleNumInPeroid = -1;

    /**
     *
     */
    private Integer hotArticleNumInPeroid = -1;

    /**
     *
     */
    private Integer totalLikeNumInPeroid = -1;
    private List<HotToken> mediaHotWords = asList();

    private Double avgHeadLikeRateInPeroid = -1000000d;
    private Double avgHeadReadRateInPeroid = -1000000d;
    private Integer avgHeadLikeRateRankInPeroid = -1000000;
    private Integer avgHeadReadRateRankInPeroid = -1000000;

    public Double getAvgHeadLikeRateInPeroid() {
        return avgHeadLikeRateInPeroid;
    }

    public void setAvgHeadLikeRateInPeroid(Double avgHeadLikeRateInPeroid) {
        this.avgHeadLikeRateInPeroid = avgHeadLikeRateInPeroid;
    }

    public Double getAvgHeadReadRateInPeroid() {
        return avgHeadReadRateInPeroid;
    }

    public void setAvgHeadReadRateInPeroid(Double avgHeadReadRateInPeroid) {
        this.avgHeadReadRateInPeroid = avgHeadReadRateInPeroid;
    }

    public Integer getAvgHeadLikeRateRankInPeroid() {
        return avgHeadLikeRateRankInPeroid;
    }

    public void setAvgHeadLikeRateRankInPeroid(Integer avgHeadLikeRateRankInPeroid) {
        this.avgHeadLikeRateRankInPeroid = avgHeadLikeRateRankInPeroid;
    }

    public Integer getAvgHeadReadRateRankInPeroid() {
        return avgHeadReadRateRankInPeroid;
    }

    public void setAvgHeadReadRateRankInPeroid(Integer avgHeadReadRateRankInPeroid) {
        this.avgHeadReadRateRankInPeroid = avgHeadReadRateRankInPeroid;
    }

    public static List<Point> DefaultPeroidPoint() {
        Calendar instance = Calendar.getInstance(Locale.CHINESE);
        instance.add(Calendar.DATE, -1);
        List<Point> list = new LinkedList<>();

        for (int i = 0; i < 30; i++) {
            instance.add(Calendar.DATE, -1);
            list.add(new Point(instance.getTimeInMillis(), 0));
        }
        return list;
    }


    /**
     *
     */
    private List<Point> headReadPoints = DefaultPeroidPoint();

    /**
     *
     */
    private List<Point> secondReadPoints = DefaultPeroidPoint();

    /**
     *
     */
    private List<Point> thirdReadPoints = DefaultPeroidPoint();

    public Integer getIsOriginal() {
        return isOriginal;
    }

    public void setIsOriginal(Integer isOriginal) {
        this.isOriginal = isOriginal;
    }

    public Integer getAvgHeadReadNumInPeroid() {
        return avgHeadReadNumInPeroid;
    }

    public void setAvgHeadReadNumInPeroid(Integer avgHeadReadNumInPeroid) {
        this.avgHeadReadNumInPeroid = avgHeadReadNumInPeroid;
    }

    public Integer getAvgHeadLikeNumInPeroid() {
        return avgHeadLikeNumInPeroid;
    }

    public void setAvgHeadLikeNumInPeroid(Integer avgHeadLikeNumInPeroid) {
        this.avgHeadLikeNumInPeroid = avgHeadLikeNumInPeroid;
    }

    public Integer getPubNumInPeroid() {
        return pubNumInPeroid;
    }

    public void setPubNumInPeroid(Integer pubNumInPeroid) {
        this.pubNumInPeroid = pubNumInPeroid;
    }

    public Integer getPubArticleNumInPeroid() {
        return pubArticleNumInPeroid;
    }

    public void setPubArticleNumInPeroid(Integer pubArticleNumInPeroid) {
        this.pubArticleNumInPeroid = pubArticleNumInPeroid;
    }

    public Integer getHotArticleNumInPeroid() {
        return hotArticleNumInPeroid;
    }

    public void setHotArticleNumInPeroid(Integer hotArticleNumInPeroid) {
        this.hotArticleNumInPeroid = hotArticleNumInPeroid;
    }

    public Integer getTotalLikeNumInPeroid() {
        return totalLikeNumInPeroid;
    }

    public void setTotalLikeNumInPeroid(Integer totalLikeNumInPeroid) {
        this.totalLikeNumInPeroid = totalLikeNumInPeroid;
    }

    public List<Point> getHeadReadPoints() {
        return headReadPoints;
    }

    List<String> itags;

    public List<String> getItags() {
        return itags;
    }

    public void setItags(List<String> itags) {
        this.itags = itags;
    }

    public void setHeadReadPoints(List<Point> headReadPoints) {
        List<Point> points = formatePoints(headReadPoints);
        this.headReadPoints = points;
    }

    private List<Point> formatePoints(List<Point> headReadPoints) {
        Calendar instance = Calendar.getInstance(Locale.CHINESE);
        instance.set(Calendar.HOUR_OF_DAY, 0);
        instance.set(Calendar.MINUTE, 0);
        instance.set(Calendar.SECOND, 0);
        instance.set(Calendar.MILLISECOND, 0);
        List<Point> list = new LinkedList<>();
        instance.add(Calendar.DATE, -1);

        for (int i = 0; i < 30; i++) {
            instance.add(Calendar.DATE, -1);
            Optional<Point> first = headReadPoints.stream().filter(point -> {
                return point.getTime() >= instance.getTimeInMillis() && point.getTime() < (instance.getTimeInMillis() + 3600 * 24 * 1000);
            }).findFirst();
            if(first.isPresent()) {
                list.add(first.get());
            }else {
                list.add(new Point(instance.getTimeInMillis(), 0));
            }
        }
        return list;

    }

    public List<Point> getSecondReadPoints() {
        return secondReadPoints;
    }

    public void setSecondReadPoints(List<Point> secondReadPoints) {
        this.secondReadPoints = formatePoints(secondReadPoints);
    }

    public List<Point> getThirdReadPoints() {
        return thirdReadPoints;
    }

    public void setThirdReadPoints(List<Point> thirdReadPoints) {
        this.thirdReadPoints = formatePoints(thirdReadPoints);
    }

    public Integer getPubOriginalArticleNumInPeroid() {
        return pubOriginalArticleNumInPeroid;
    }

    public void setPubOriginalArticleNumInPeroid(Integer pubOriginalArticleNumInPeroid) {
        this.pubOriginalArticleNumInPeroid = pubOriginalArticleNumInPeroid;
        if (pubOriginalArticleNumInPeroid > 0) {
            this.isOriginal = 1;
        }
    }

    public List<HotToken> getMediaHotWords() {
        return mediaHotWords;
    }

    public void setMediaHotWords(List<HotToken> mediaHotWords) {
        this.mediaHotWords = mediaHotWords;
    }


}
