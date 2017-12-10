package com.ptb.gaia.tool;

import com.ptb.gaia.tool.command.*;
import com.ptb.gaia.tool.emchat.chatMessage.EmChatPullEntry;
import com.ptb.gaia.tool.emchat.convertUser.EmchatConvertEntry;
import com.ptb.gaia.tool.esTool.convert.EsArticleConvert;
import com.ptb.gaia.tool.esTool.convert.EsMediaConvert;
import com.ptb.gaia.tool.esTool.search.SearchArticle;
import com.ptb.gaia.tool.esTool.util.MediaTags;
import org.apache.commons.cli.*;

import java.util.Arrays;

import static com.ptb.gaia.tool.command.TestCommand.testArticle;
import static com.ptb.gaia.tool.command.TestCommand.testMedia;

/**
 * Created by eric on 16/7/1.
 */
//@SpringBootApplication
//@ComponentScan
//@ImportResource("classpath:dubbo_consumer.xml")
public class Tool {

	public static void main(String[] args) throws Exception {
		CommandLineParser parser = new BasicParser();
		Options options = new Options();
		options.addOption("h", "help", false, "显示帮助信息");
		options.addOption("c5", true, "读取文章接口测试");
		options.addOption("c6", true, "读取媒体接口测试");
		options.addOption("c7", true, "将第三方微信认证信息导入到媒体库中");
		options.addOption("c8", true, "将运营配置的媒体标签导入ES，参数为path1 arg1 path2 arg2");
		options.addOption("i", "inPath", true, "csv格式,格式问厉超");
		options.addOption("o", "outputpath", true, "为c8参数");
		options.addOption("c9", "", false, "将标签数据导入微博媒体库");
		options.addOption("endTime", true, "es导入数据的下边界,如不指定,则为当前时间");
		options.addOption("export_media_detail", true, "导出指定的媒体数据 1微信,2微博  " +
			" mysql -h172.16.0.211 -udeveloper -pptb-yanfa-users zeus -e \" select a.pmid,a.type_id,b.name from media_tag a join default_interest b on a.type_id = b.type_id;\"|./bin/tools.sh -export_media_detail 2|sed 's/[[:blank:]]\\{1,10\\}/,/g'" +
			"select substring(url,33,16),name,i_id,b.id from article_channel_detail a join article_channel b on  a.acid = b.id limit 1;");
		options.addOption("export_media_detail", true, "导出指定的媒体数据 1微信,2微博");
		options.addOption("export_lost_media", false, "补入遗失媒体");
		options.addOption("ft", false, "快速文本分析器");
		options.addOption("libPath", true, "分类库路径");
		options.addOption("modelPath", true, "分类模型路径");
		options.addOption("dp", false, "(分类的测试程序)Data preparation(数据准备),将数据输入：pmid 分类id  输出: 标签 , 分词结果(空格分割)");
		options.addOption("agedArticle", true, "老化MONGO及ES中无用的文章 参数为时间戳如【1474532209000】切记！输对时间戳");
		options.addOption("emchatImport", false, "将本地用户导入到第三方交互平台(环信)");
		options.addOption("emchatExport", false, "从第三方平台(环信)拉取聊天信息到本地mongo");
		options.addOption("contacts", false, "得到联系方式 contacts");
		options.addOption("agedMedia", true, "老化MONGO的媒体信息 参数为时间戳如【1474532209000】切记！输对时间戳");
		options.addOption("platType", true, "媒体类型");
		options.addOption("k", "import_mediaDataToEsOnce", true, "单次导入媒体数据到ES(程序只执行一次导入全部数据工作)");
		options.addOption("y", "import_mediaDataToEsCycle", false, "持续导入媒体数据到ES(程序会持续运行并间隔一定时间(配置文件中设置)执行一次导入最新媒体数据,日志中有标示最后更新时间)");
		options.addOption("d", "importArticleToEs", true, "单次导入文章(包括文章标题和内容同时导入), 需要两个参数, 第一个是类型1是微信,2是微博, 第二个是开始时间的unix毫秒时间戳,{顺序不能颠倒}");
		options.addOption("importArticleToEsIncrement", false, "增量导入文章标题");

		options.addOption("wbMediasCopy", false, "微博媒体备份");
		options.addOption("wbArticlesCopy", true, "微博文章备份");
		options.addOption("wxMediasCopy", false, "微信媒体备份");
		options.addOption("wxArticlesCopy", true, "微信文章备份");
		options.addOption("mediaTagImport2Es", true, "媒体标签导入ES");
		options.addOption("timeEnd", true, "结束时间戳 为13位时间戳");
		options.addOption("wxArticleReload", false, "微信文章补录");
		options.addOption("wxMediaReload", false, "微信媒体补录");
		options.addOption("wbArticleReload", false, "微博文章补录");
		options.addOption("wxArticleStaticReload", false, "微信文章静态数据补录-2017-03-28");
		options.addOption("scheduleClean", false, "调度清洗");
		options.addOption("articlePmidNotInMediaMongo",false,"文章对应媒体不存在查询");
		options.addOption("wxwbMediaToRedis", false, "将新库媒体数据导入redis 作为白名单 爬虫使用");
		options.addOption("FilePathToMongoDB", false, "将微信没有filepath字段的mongodb 文章添加 这个字段  并且 ftp添加wx文章内容  content截取200字符");
		options.addOption("startTime", true, "通用 开始时间");
		options.addOption("endTime", true, "通用 结束时间");
		options.addOption("startHour", true, "通用 开始时间 小时");
		options.addOption("endHour", true, "通用 结束时间 小时");
		options.addOption("mediaMissingExport", true, "媒体在库中是否存在！");
		options.addOption("missingWxMedia2Mongo", true, "遗失微信媒体爬取并导入mongo库中");
		options.addOption("missingWbMedia2Mongo", true, "遗失微博媒体爬去并导入mongo库中");
		options.addOption("media_info_schedule", true, "微信媒体添加到爬虫调度, 参数：platType,pmid");
		options.addOption("s", "searchArticleTest", true, "查询文章的测试工具,提供给产品使用。");
	  	options.addOption("wxad3schedule", true, "微信文章动态数据异常的文章重新添加调度, 参数为url");
	    options.addOption("wbScheduleInfo", true, "微博调度包含数据查询");
	  	options.addOption("mediaArticle2hdfs", true, "媒体文章导入到hdfs");
		options.addOption("agedWxWbArticle", false, "清洗数据二期");
		options.addOption("update_wb_article_posttime", false, "微博文章发布时间更新");
		options.addOption("PillDataToMongodb", false, "将mysql数据库中的article_channle_detail表中的url补到mongodb");
		options.addOption("update_wx_media", true, "更新mongodb wxMedia 根据 mysql ptb_product表微信媒体");
		options.addOption("insert_wx_media",true,"添加微信媒体到mongondb 更具微信文章添加");
		options.addOption("add_uranus_schedule", true, "添加爬虫调度,参数为存放pmid文件地址，添加类型两个参数间用逗号隔开");

		CommandLine commandLine = null;
		HelpFormatter formatter = new HelpFormatter();
		try {
			commandLine = parser.parse(options, args);
		} catch (Exception e) {
			formatter.printHelp("usage:", options);
			e.printStackTrace();
		}
		if (commandLine.hasOption('h')) {
			formatter.printHelp("usage:", options);
			System.exit(-1);
		}
		if (commandLine.hasOption("c5")) {
			String c5 = commandLine.getOptionValue("c5");
			testArticle(Integer.valueOf(c5));
		} else if (commandLine.hasOption("c6")) {
			String c5 = commandLine.getOptionValue("c6");
			testMedia(Integer.valueOf(c5));
		} else if (commandLine.hasOption("c7")) {
			String path = commandLine.getOptionValue("c7");
			UpdateCommand.BayouWxMediaAuthen.syncBayouWxVerify(path);
		} else if (commandLine.hasOption("c8")) {
			String path1 = commandLine.getOptionValue("i");
			String path2 = commandLine.getOptionValue("o");
			TagCommand.start(path1, path2);
		} else if (commandLine.hasOption("c9")) {
			WeiboCommand.ImportWbTagToMongo();
		}else if (commandLine.hasOption("import_mediaDataToEsOnce")) {
			String type = commandLine.getOptionValue("import_mediaDataToEsOnce");
			EsMediaConvert.startTask(0, Integer.parseInt(type));
		} else if (commandLine.hasOption("import_mediaDataToEsCycle")) {
			EsMediaConvert.startTask(1, 0);
		} else if (commandLine.hasOption("export_media_detail")) {
			long platForm = Long.valueOf(commandLine.getOptionValue("export_media_detail"));
			MediaCommand.getMediaDetail(platForm);
		} else if (commandLine.hasOption("export_lost_media")) {
			MediaCommand.getLostMedia();
		} else if (commandLine.hasOption("ft")) {
			String modelPath = commandLine.getOptionValue("modelPath");
			ArticleCommond.category(commandLine.getOptionValue("libPath"), modelPath);
		} else if (commandLine.hasOption("dp")) {
			ArticleCommond ac = new ArticleCommond();
			ac.dataPrepar();
		} else if (commandLine.hasOption("agedArticle")) {
			long timeLimit = Long.parseLong(commandLine.getOptionValue("agedArticle"));
			ArticleCommond.agedArticle(timeLimit);
		} else if (commandLine.hasOption("emchatImport")) {
			EmchatConvertEntry.emchatConvertTask();
		} else if (commandLine.hasOption("emchatExport")) {
			EmChatPullEntry.emchatExport();
		} else if (commandLine.hasOption("contacts")) {
			MediaCommand.getAllMediaConcact();
		} else if (commandLine.hasOption("agedMedia")) {
			int type = Integer.valueOf(commandLine.getOptionValue("platType"));
			long timeLimit = Long.parseLong(commandLine.getOptionValue("agedMedia"));
			ArticleCommond.agedMedia(timeLimit, type);
		}else if (commandLine.hasOption("importArticleToEs")) {
			String[] typeList = commandLine.getOptionValues("importArticleToEs");
			if (typeList.length < 2) {
				return;
			}
			EsArticleConvert.articleEntry(Integer.valueOf(typeList[0]), Long.valueOf(typeList[1]));
		} else if (commandLine.hasOption("importArticleToEsIncrement")) {
			EsArticleConvert.ArticleIncrementEntry();
		} else if (commandLine.hasOption("wbMediasCopy")) {
			new WbMediaClean().handleMedias();
		} else if (commandLine.hasOption("wbArticlesCopy")) {
			long timeLimit = Long.parseLong(commandLine.getOptionValue("wbArticlesCopy"));
			long timeEnd = Long.parseLong(commandLine.getOptionValue("timeEnd"));
			new WbMediaClean().handleArticles(timeLimit, timeEnd);
		} else if (commandLine.hasOption("wxMediasCopy")) {
			new WxMediaClean().handleMedias();
		} else if (commandLine.hasOption("wxwbMediaToRedis")) {
			int type = Integer.valueOf(commandLine.getOptionValue("platType"));
			if(type==1){
				new WxMediaClean().wxMediaToRedis();
			}else{
				new WxMediaClean().wbMediaToRedis();
			}
		} else if (commandLine.hasOption("wxArticlesCopy")) {
			long timeLimit = Long.parseLong(commandLine.getOptionValue("wxArticlesCopy"));
			long timeEnd = Long.parseLong(commandLine.getOptionValue("timeEnd"));
			new WxMediaClean().handleArticles(timeLimit, timeEnd);
		} else if (commandLine.hasOption("mediaTagImport2Es")) {
			String esPath = commandLine.getOptionValue("esPath");
			MediaTags mediaTags = new MediaTags();
			mediaTags.importTags(esPath);
		} else if (commandLine.hasOption("wxArticleReload")) {
			new WxArticleReload().handleArticles();
		} else if (commandLine.hasOption("wxMediaReload")) {
			new WxArticleReload().handleMedias();
		} else if (commandLine.hasOption("wbArticleReload")) {
			new WxArticleReload().handleWbArticles();
		} else if (commandLine.hasOption("FilePathToMongoDB")) {
			long startTime = Long.parseLong(commandLine.getOptionValue("startTime"));
			long Endtime = Long.parseLong(commandLine.getOptionValue("endTime"));
			int startHour = Integer.parseInt(commandLine.getOptionValue("startHour"));
			int EndHour = Integer.parseInt(commandLine.getOptionValue("endHour"));
			new WxMediaClean().wxArticleAddFilePath(startTime, Endtime,startHour,EndHour);
		} else if (commandLine.hasOption("wxArticleStaticReload")) {
			new WxArticleReload().wxArticleStaticsAdd(commandLine.getOptionValue("wxArticleStaticReload"));
		} else if (commandLine.hasOption("scheduleClean")) {
			WxArticleReload wxArticleReload = new WxArticleReload();
			wxArticleReload.scheduleClean("C_WB_A_N", "gaia2", "wbMedia");
			wxArticleReload.scheduleClean("C_WX_A_N", "gaia2", "wxMedia");
		} else if (commandLine.hasOption("articlePmidNotInMediaMongo")) {
			WxMediaClean articlePmidNotInMediaMongo = new WxMediaClean();
			articlePmidNotInMediaMongo.articlePmidNotInMediaMongoWx();
			articlePmidNotInMediaMongo.articlePmidNotInMediaMongoWb();
		} else if (commandLine.hasOption("mediaMissingExport")) {
			String paths = commandLine.getOptionValue("mediaMissingExport");
			String[] split = paths.split(",");
			NewMediaCommand.getMissingPmids(split[0], split[1], split[2], split[3]);
		} else if (commandLine.hasOption("missingWxMedia2Mongo")) {
			String inputPath = commandLine.getOptionValue("missingWxMedia2Mongo");
			NewMediaCommand.missingWXMedia2Mongo(inputPath);
		} else if (commandLine.hasOption("missingWbMedia2Mongo")) {
			String inputPath = commandLine.getOptionValue("missingWbMedia2Mongo");
			NewMediaCommand.missingWBMedia2Mongo(inputPath);
		} else if (commandLine.hasOption("media_info_schedule")) {
			String input = commandLine.getOptionValue("media_info_schedule");
			String[] arg = input.split(",");
			if (arg.length < 2) System.out.println("参数错误：格式-> platType,pmid");
			int platType = Integer.parseInt(arg[0]);
			String pmid = arg[2];
			NewMediaCommand.uranusScheduleAddMedias(platType, pmid);
		}else if (commandLine.hasOption("searchArticleTest")){
			String[] searchArticleTests = commandLine.getOptionValues("searchArticleTest");
			if (searchArticleTests.length < 3 || !searchArticleTests[1].matches("[0-9]+") || !searchArticleTests[2].matches("[0-9]+")){
				//formatter.printHelp("usage:", options);
				System.out.print("需要3个参数,后两个必须是数字");
				System.exit(-1);
			}
			SearchArticle.searchArticleTest(searchArticleTests[0], Integer.parseInt(searchArticleTests[1]), Integer.parseInt(searchArticleTests[2]));
		} else if(commandLine.hasOption("wxad3schedule")){
		  	WxArticleReload wxArticleReload = new WxArticleReload();
			wxArticleReload.wxA2Schedule(commandLine.getOptionValue("wxad3schedule"));
		} else if(commandLine.hasOption("wbScheduleInfo")){
		    String opt = commandLine.getOptionValue("wbScheduleInfo");
		  	String[] arg = {opt};
		  	ScheduleCommand.main(arg);
		}else if(commandLine.hasOption("mediaArticle2hdfs")){
		  	String opt = commandLine.getOptionValue("mediaArticle2hdfs");
		    String[] split = opt.split(",");
		  	String[] types = {split[4]};
		  	if (split[4].contains(":::") )types = split[4].split(":::");
			Arrays.stream(types).parallel().forEach(typeId ->
			MediaArticle2Hdfs.save(Long.parseLong(split[0]), Integer.parseInt(split[1]), split[2], Boolean.valueOf(split[3]), typeId));
		} else if (commandLine.hasOption("agedWxWbArticle")) {
			int type = Integer.valueOf(commandLine.getOptionValue("platType"));
			ArticleCommond.agedWxWbArticle(type);
		}else if (commandLine.hasOption("PillDataToMongodb")) {
			ArticleCommond.PillDataToMongodb();
		}
		else if (commandLine.hasOption("update_wb_article_posttime")){
			WeiboCommand.update();
		}else if (commandLine.hasOption("update_wx_media")){
            String update_wx_media = commandLine.getOptionValue("update_wx_media");
            UpdateWxMedia.getWxMediaDetail(update_wx_media);
		}else if (commandLine.hasOption("insert_wx_media")){
			String insert_wx_media = commandLine.getOptionValue("insert_wx_media");
			UpdateWxMedia.updateWxMediaByWxArticleUrl(insert_wx_media);
		}else if(commandLine.hasOption("add_uranus_schedule")){
		    String scheduleArgs = commandLine.getOptionValue("add_uranus_schedule");
			String[] split = scheduleArgs.split(",");
			NewMediaCommand.addSchedule(split[0], split[1]);
		} else {
			formatter.printHelp("usage:", options);
		}

	}
}
