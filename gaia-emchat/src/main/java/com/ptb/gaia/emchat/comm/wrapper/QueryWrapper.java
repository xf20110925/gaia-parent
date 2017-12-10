package com.ptb.gaia.emchat.comm.wrapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

public class QueryWrapper {
	
	private static final String LIMIT = "limit";
	
	private static final String CURSOR = "cursor";

	private static final String QUERY = "ql";
	
	private List<NameValuePair> queries = new ArrayList<NameValuePair>();
	
	public static QueryWrapper newInstance() {
		return new QueryWrapper();
	}
	
	public QueryWrapper addQuery(String key, String value) {
		if( StringUtils.isBlank(key) || StringUtils.isBlank(value) ){
			return this;
		}
		
		queries.add(new BasicNameValuePair(key, value));
		return this;
	}
	
	public QueryWrapper addLimit(Long limit) {
		limit = null == limit ? 10L : limit;

		return addQuery(LIMIT, limit.toString());
	}
	
	public QueryWrapper addCursor(String cursor) {
		return addQuery(CURSOR, cursor);
	}

	public QueryWrapper addQueryLang(String ql) {
		return addQuery(QUERY, ql);
	}

	public List<NameValuePair> getQueries() {
		return queries;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for( NameValuePair query : queries ) {
			sb.append("[").append(query.getName()).append(":").append(query.getValue()).append("] ");
		}
		
		return sb.toString();
	}

	public String toUri(){
		StringBuffer sb = new StringBuffer();
		for( NameValuePair query : queries ) {
			sb.append("&").append(query.getName()).append("=").append(this.encodeUrl(query.getValue(), 2));
		}

		return sb.toString().substring(1, sb.toString().length());
	}

	public static String encodeUrl(String src, int type){
		String gb2312 = null;
		try {
			if (type == 1){
				gb2312 = URLEncoder.encode(src, "gb2312");
			}else if (type == 2){
				gb2312 = URLEncoder.encode(src, "utf8");
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		return gb2312;
	}
}
