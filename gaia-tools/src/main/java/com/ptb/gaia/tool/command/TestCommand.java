package com.ptb.gaia.tool.command;

import com.ptb.gaia.service.IGaia;
import com.ptb.gaia.service.IGaiaImpl;
import com.ptb.gaia.service.RankType;
import com.ptb.gaia.service.entity.article.GArticleSet;
import com.ptb.gaia.service.entity.article.GWbArticle;
import com.ptb.gaia.service.entity.media.GMediaSet;
import com.ptb.gaia.service.entity.media.GWxMedia;

import org.apache.commons.configuration.ConfigurationException;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by eric on 16/8/10.
 */
public class TestCommand {

	public static void testMedia(Integer integer) {
		IGaia iGaia = null;
		try {
			iGaia = new IGaiaImpl();
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
		ExecutorService executorService = Executors.newFixedThreadPool(integer);
		long startTime = System.currentTimeMillis();
		int count = 50000;
		AtomicInteger i = new AtomicInteger(0);
		int n = integer;
		while (n-- > 0) {
			IGaia finalIGaia = iGaia;
			executorService.execute(() -> {
				while (i.get() < count) {
					GMediaSet<GWxMedia> wxHotMedia = finalIGaia.getWxHotMedia(weiboDefaultMedia, 0, 3, RankType.RANK_VERG_COMMENT);
					int i1 = i.incrementAndGet();
					if (i1 % 1000 == 0) {
						System.out.println(String.format("process %d 's record", i.get()));
					}
				}
				long endTime = System.currentTimeMillis();
				System.out.println(String.format("%f/s cost %d's", count / ((endTime - startTime) / 1000.0), endTime - startTime));
			});
		}

		while (i.get() < count) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	static List<String> weixinDefaultMedia = Arrays.asList("MzAwMDAyMzY3OA==", "MzA4MDE4OTYxNA==", "MjM5MjEzNzYzOA==");
	static List<String> weiboDefaultMedia = Arrays.asList("1764222885", "1266321801", "2714280233");

	public static void testArticle(Integer integer) {
		IGaia iGaia = null;
		try {
			iGaia = new IGaiaImpl();
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
		ExecutorService executorService = Executors.newFixedThreadPool(integer);
		long startTime = System.currentTimeMillis();
		int count = 50000;
		AtomicInteger i = new AtomicInteger(0);
		int n = integer;
		while (n-- > 0) {
			IGaia finalIGaia = iGaia;
			int finalN = n;
			executorService.execute(() -> {
				while (i.get() < count) {
					GArticleSet<GWbArticle> wxHotArticles = finalIGaia.getWbRecentArticles(weiboDefaultMedia, 0, 10);
					int i1 = i.incrementAndGet();
					if (i1 % 1000 == 0) {
						System.out.println(String.format("process %d 's record", i.get()));
					}
				}
				long endTime = System.currentTimeMillis();
				System.out.println(String.format("%f/s cost %d's", count / ((endTime - startTime) / 1000.0), endTime - startTime));
			});
		}

		while (i.get() < count) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
