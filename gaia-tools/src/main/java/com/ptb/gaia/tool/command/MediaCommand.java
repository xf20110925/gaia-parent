package com.ptb.gaia.tool.command;

import com.alibaba.fastjson.JSON;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.ptb.gaia.service.IGaia;
import com.ptb.gaia.service.IGaiaImpl;
import com.ptb.gaia.service.entity.media.GWbMedia;
import com.ptb.gaia.service.entity.media.GWxMedia;
import com.ptb.gaia.service.service.GWbMediaService;
import com.ptb.gaia.service.service.GWxMediaService;
import com.ptb.gaia.service.utils.ConvertUtils;
import com.ptb.gaia.tool.esTool.util.MysqlDb;
import com.ptb.uranus.spider.weibo.WeiboSpider;
import com.ptb.uranus.spider.weibo.bean.WeiboAccount;
import com.ptb.uranus.spider.weibo.bean.WeiboSearchAccount;
import com.ptb.uranus.spider.weixin.WeixinSpider;
import com.ptb.uranus.spider.weixin.bean.GsData;
import com.ptb.uranus.spider.weixin.bean.WxAccount;
import com.ptb.utils.string.RegexUtils;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;

import static java.lang.Thread.sleep;

/**
 * Created by eric on 16/9/5.
 */
public class MediaCommand {

	static Logger logger = Logger.getLogger(MediaCommand.class);

	public static IGaia iGaia;
	public static WeixinSpider weixinSpider = new WeixinSpider();
	public static WeiboSpider weiboSpider = new WeiboSpider();

	static {
		try {
			iGaia = new IGaiaImpl();
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}

	}

	public static void getMediaDetail(long platForm) {
		Scanner scanner = new Scanner(System.in);
		while (scanner.hasNext()) {
			try {
				String s = scanner.nextLine();
				String[] split = s.split("\\s+", 2);
				String pmid = split[0];
				String other = "";
				if (split.length > 1) {
					other = split[1];
				}
				switch (Math.toIntExact(platForm)) {
					case 1:

						GWxMedia wxMedia = iGaia.getWxMedia(pmid);
						if (StringUtils.isBlank(wxMedia.getBrief())) {
							wxMedia.setBrief("");
						}
						System.out.printf("%s\t%s\t%d\t%d\t%d\t%s\t%s\n", pmid, wxMedia.getMediaName(), wxMedia.getIsOriginal(), wxMedia.getAvgHeadReadNumInPeroid(), wxMedia.getAvgHeadLikeNumInPeroid(), StringEscapeUtils.unescapeHtml4(wxMedia.getBrief()).replaceAll("\\s", " ").replaceAll("\\n", " ").replaceAll("\\r", " "), other);
						break;
					case 2:
						GWbMedia wbMedia = iGaia.getWbMedia(pmid);
						if (StringUtils.isBlank(wbMedia.getBrief())) {
							wbMedia.setBrief("");
						}
						System.out.printf("%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\t%s\n", pmid, wbMedia.getMediaName(), wbMedia.getIsOriginal(), wbMedia.getFansNum(), wbMedia.getAvgForwardNumInPeroid(), wbMedia.getAvgCommentNumInPeroid(), wbMedia.getAvgLikeNumInPeroid(), StringEscapeUtils.unescapeHtml4(wbMedia.getBrief()).replaceAll("\\s", " ").replaceAll("\\n", " ").replaceAll("\\r", " "), other);
						break;
					default:
				}
			} catch (Exception e) {

			}
		}
	}

	public static void getLostMedia() {
		while (true) {
			MysqlDb mysqlDb = new MysqlDb();
			List<Map<String, String>> lostMediaList = mysqlDb.getLostMediaList("select * from lost_media where status=0 order by add_time desc");
			if (lostMediaList.size() < 1) {
				logger.info("logst_media crawler mediaNameList   " + lostMediaList.size());
			} else {
				lostMediaList.forEach(lost -> {
					String name = lost.get("media_name");
					logger.info("lost_media crawler mediaName" + name);
					try {
						Optional<List<GsData>> media_name = weixinSpider.getWeixinAccountByIdOrName(name, 1);
						WxAccount wxAccount = new WxAccount();
						String url = media_name.get().get(0).getIncluded();
						String biz = RegexUtils.sub(".*__biz=([^#&]*).*", url, 0);
						wxAccount.setBiz(biz);
						wxAccount.setBrief(media_name.get().get(0).getFunctionintroduce());
						wxAccount.setHeadImg(media_name.get().get(0).getHeadportrurl());
						wxAccount.setNickName(media_name.get().get(0).getWechatname());
						wxAccount.setQcCode(media_name.get().get(0).getQrcodeurl());
						GWxMediaService g = new GWxMediaService();
						g.insert(ConvertUtils.convert2GWxMedia(wxAccount));
					} catch (Exception e) {
						e.printStackTrace();
						logger.info("lost_media wx crawler MediaName" + name);
					}

					try {
						Optional<List<WeiboSearchAccount>> weibo = weiboSpider.getWeiboSerachAccountByName(name);
						if (weibo != null) {
							WeiboAccount weiboAccount = new WeiboAccount();
							WeiboSearchAccount wb = weibo.get().get(0);
							if (wb.getAccount().equals(name) || wb.getAccount().equals(name)) {
								weiboAccount.setNickName(wb.getAccount());
								weiboAccount.setHeadImg(wb.getHeadportrait());
								weiboAccount.setActivePlace(wb.getPersonalpage());
								weiboAccount.setWeiboID(wb.getWeiboId());
								weiboAccount.setAttNum(Integer.parseInt(wb.getWeibo()));
								weiboAccount.setCreate_time(System.currentTimeMillis());
								weiboAccount.setDesc(wb.getIntroduction());
								weiboAccount.setFansNum(Integer.parseInt(wb.getFans()));
								weiboAccount.setFavourites_count(Integer.parseInt(wb.getFocus()));
								weiboAccount.setGender(wb.getGender());
								if (wb.getVertifyType() == null || wb.getVertifyType().equals("")) {
									weiboAccount.setVerifiedType("0");
								} else {
									weiboAccount.setVerifiedType(wb.getVertifyType());
								}
								GWbMediaService gWxMediaService = new GWbMediaService();
								gWxMediaService.insertBatch(Arrays.asList(ConvertUtils.convert2GWbMedia(weiboAccount)));
								gWxMediaService.insertBatch(Arrays.asList(ConvertUtils.ConverDynamic(weiboAccount)));
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
						logger.info("lost_media wb crawler MediaName" + name);
					}

					mysqlDb.updateLostMediaStatus(Integer.parseInt(lost.get("id")));

				});
			}

			try {
				sleep(10000l);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}


	static class contact {
		String phone;
		String email;
		String qq;
		String weixin;

		public contact() {
		}

		public contact(String phone, String email, String qq, String wx) {
			this.phone = phone == null ? "" : phone;
			this.email = email == null ? "" : email;
			this.qq = qq == null ? "" : qq;
			this.weixin = wx == null ? "" : wx;

		}

		public String getPhone() {
			return phone;
		}

		public void setPhone(String phone) {
			this.phone = phone;
		}

		public String getEmail() {
			return email;
		}

		public void setEmail(String email) {
			this.email = email;
		}

		public String getQq() {
			return qq;
		}

		public void setQq(String qq) {
			this.qq = qq;
		}

		public String getWeixin() {
			return weixin;
		}

		public void setWeixin(String weixin) {
			this.weixin = weixin;
		}


		@Override
		public String toString() {
			return new ToStringBuilder(this).append("phone", phone).append("email", email).append("qq", qq).append("weixin", weixin).toString();
		}

		public boolean isNotALLNull() {
			if (StringUtils.isBlank(qq) && StringUtils.isBlank(weixin) && StringUtils.isBlank(phone) && StringUtils.isBlank(email)) {
				return false;
			}
			return true;
		}
	}

	public static contact parseConcact(String brief) {
		String phone = RegexUtils.sub("[^\\d]*(1[^0^1^2][0-9]{9})[^\\d]*", brief, 0);
		String email = RegexUtils.sub("[^\\w]*(\\w{4,20}@\\w+\\.\\w+)[^\\w]*", brief, 0);
		String qq = null;
		String wx = null;
		String lowerCaseBrief = brief.toLowerCase();
		if (lowerCaseBrief.contains("qq")) {
			qq = RegexUtils.sub("[^\\d]*([0-9]{5,12})[^\\d]*", lowerCaseBrief.replaceAll(".*qq", ""), 0);
		}
		if (lowerCaseBrief.contains("微信") || lowerCaseBrief.contains("weixin") || lowerCaseBrief.contains("wechat")) {
			wx = RegexUtils.sub("[^\\w]*(\\w+{5,20})[^\\w]*", lowerCaseBrief.replaceAll(".*微信", "").
					replaceAll(".*wechat", "").replaceAll(".*weixin", ""), 0);
		}

		return new contact(phone, email, qq, wx);
	}

	public static void main(String[] args) throws ConfigurationException {
		getAllMediaConcact();
	}

	public static String parseContact(GWbMedia gWbMedia) {
		if (gWbMedia != null && gWbMedia.getBrief() != null) {
			contact contact = parseConcact(gWbMedia.getBrief());
			if (contact.isNotALLNull()) {
				System.out.printf("%s,%s,%s,%s,%s,%s,%s\n", gWbMedia.getPmid(), contact.getPhone(), contact.getEmail(), contact.getQq(), contact.getWeixin(),gWbMedia.getMediaName(),
								  gWbMedia.getBrief());
			}
		}
		return null;
	}

	public static void getAllMediaConcact() throws ConfigurationException {
		int i = 0;
		GWbMediaService gWbMediaService = new GWbMediaService();
		MongoCollection<Document> wbMediaCollect = gWbMediaService.getWbMediaCollect();
		FindIterable<Document> documents = wbMediaCollect.find();
		for (Document document : documents) {
			i++;
			GWbMedia gWbMedia = JSON.parseObject(JSON.toJSONString(document), GWbMedia.class);
			parseContact(gWbMedia);
			if(i % 1000 == 0) {
				System.err.println(String.format("已处理%d条数据", i));
			}
		}
	}
}
