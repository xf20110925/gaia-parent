package com.ptb.gaia.tool.command;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.ptb.gaia.service.service.GWbArticleService;
import com.ptb.gaia.service.service.GWbMediaService;

import com.ptb.gaia.tool.utils.MongoUtils1;
import org.apache.commons.configuration.ConfigurationException;
import org.bson.Document;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @DESC:
 * @VERSION:
 * @author: xuefeng
 * @Date: 2016/11/3
 * @Time: 13:29
 */
public class WbMediaClean {
	public static final int fansNum = 10000;
	public static final String newMCollecName = "wbMedia1";
	public static final String newACollecName = "wbArticle1";
	private GWbMediaService wbMservice;
	private GWbArticleService wbAservice;
	private ExecutorService executor;

	public WbMediaClean() throws ConfigurationException {
		executor = Executors.newFixedThreadPool(8);
		wbMservice = new GWbMediaService();
		wbAservice = new GWbArticleService();
	}

	public void handleMedias() {
		AtomicLong counter = new AtomicLong(0);
		LinkedList<Document> docList = new LinkedList<>();
		FindIterable<Document> docs = wbMservice.getWbMediaCollect().find(Filters.or(Filters.eq("isUsed", 2), Filters.gt("fansNum", fansNum)));
		for (Document doc : docs) {
			counter.addAndGet(1);
			docList.add(doc);
			if (docList.size() >= 5000) {
				LinkedList<Document> threadList = new LinkedList<>(docList);
				executor.execute(() -> wbMservice.getCollectByName(newMCollecName).insertMany(threadList));
				docList.clear();
				System.out.println(String.format("已导出%s个媒体", counter));
			}
		}
		//收尾
		LinkedList<Document> lastList = new LinkedList<>(docList);
		executor.execute(() -> wbMservice.getCollectByName(newMCollecName).insertMany(lastList));
		executor.shutdown();
		ExecutorStatus(executor);
	}


	static AtomicLong counter = new AtomicLong(0);

	public void handleArticles(Long timeLimit,Long timeEnd) {
		LinkedList<String> docList = new LinkedList<>();
		//查询新collection中的媒体pmid
		FindIterable<Document> mDocs = wbMservice.getWbMediaCollect().find().projection(new Document().append("_id", 1));
		for (Document mDoc : mDocs) {
			docList.add(mDoc.getString("_id"));
			if (docList.size() >= 10) {
				insertArticles(docList,timeLimit,timeEnd);
				docList.clear();
			}
		}
		//收尾
		insertArticles(docList,timeLimit,timeEnd);
		executor.shutdown();
		ExecutorStatus(executor);
	}

	private void ExecutorStatus(ExecutorService executor) {
		while (true) {
			if (executor.isTerminated()) {
				System.out.println("所有任务执行完毕");
				break;
			}
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void insertArticles(LinkedList<String> docList,Long timeLimit,Long timeEnd) {
		LinkedList<String> pmids = new LinkedList<>(docList);
		executor.execute(() -> {
			//媒体文章
			FindIterable<Document> aDocs = null;
			try {
				aDocs = MongoUtils1.instance().getDatabase("gaia2").getCollection("wbArticleTest1").find(Filters.and(Filters.in("pmid", pmids), Filters.gt("postTime", timeLimit),Filters.lt("postTime", timeEnd)));
			} catch (ConfigurationException e) {
				e.printStackTrace();
			}
			LinkedList<Document> aList = new LinkedList<>();
			for (Document aDoc : aDocs) {
				aList.add(aDoc);
				counter.addAndGet(1);
			}
			if (aList.isEmpty()) return;
			wbAservice.getWbArticleCollect().insertMany(aList);
			System.out.println(String.format("已导出%s篇文章", counter));
		});
	}

	public Long getTime(String pattern, int days) {
		Calendar calendar = Calendar.getInstance();
    SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		try {
			calendar.setTime(sdf.parse(sdf.format(new Date())));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		calendar.add(Calendar.DAY_OF_YEAR, days);
    return calendar.getTimeInMillis();
	}

	public static void main(String[] args) throws ConfigurationException {
		WbMediaClean wbMediaClean = new WbMediaClean();
//						wbMediaClean.handleMedias();
//		wbMediaClean.handleArticles();
	}

}
