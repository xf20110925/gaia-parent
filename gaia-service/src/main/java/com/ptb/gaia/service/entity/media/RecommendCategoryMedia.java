package com.ptb.gaia.service.entity.media;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;
import java.util.Locale;

/**
 * Created by eric on 16/8/3.
 */

public class RecommendCategoryMedia {
	String pmid;
	int rank;
	String category;
	int type;
	String medianame;
	long addTime = System.currentTimeMillis();
	String date = DateFormatUtils.format(new Date(),"yyyyMMdd",Locale.CHINA);

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public long getAddTime() {
		return addTime;
	}

	public void setAddTime(long addTime) {
		this.addTime = addTime;
	}

	public String getPmid() {
		return pmid;
	}

	public void setPmid(String pmid) {
		this.pmid = pmid;
	}

	public int getRank() {
		return rank;
	}

	public void setRank(int rank) {
		this.rank = rank;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getMedianame() {
		return medianame;
	}

	public void setMedianame(String medianame) {
		this.medianame = medianame;
	}
}
