package com.ptb.gaia.tool.command;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.ptb.gaia.tool.utils.MysqlClient;
import com.ptb.uranus.common.entity.CollectType;

import org.bson.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @DESC:
 * @VERSION:
 * @author: xuefeng
 * @Date: 2016/12/27
 * @Time: 11:12
 */
public class ScheduleCommand {

  private static Map<String, Object> queryFromMongo(CollectType collctType) {
	FindIterable<Document> docs = WxArticleReload.MongoUtil.INSTACENEWMONGO.getDatabase("uranus").getCollection("schedule").find(Filters.eq("obj.collectType", collctType.name())).projection(new Document("obj.conditon", 1));
	Map<String, Object> scheduleMap = new HashMap<>();
	for (Document doc : docs) {
	  scheduleMap.put(((Document) doc.get("obj")).getString("conditon").split(":::")[0], doc.get("_id"));
	}
	return scheduleMap;
  }

  private static List<String> queryFromMysql(int platType) {
	String sql = "SELECT p_mid FROM user_media where type = ?";
	List<String> pmids = MysqlClient.instance.queryForList(sql, String.class, platType);
	return pmids;
  }

  public static void query(CollectType collectType, int platType) {
	List<String> pmids = queryFromMysql(2);
	System.out.println(String.format("用户收藏%s媒体数量->%d", platType == 2 ? "微博" : platType == 1 ? "微信" : "输入平台值不合法", pmids.size()));
	Map<String, Object> pmidMap = queryFromMongo(collectType);
	System.out.println(String.format("调度->%s数量->%d", collectType.name(), pmidMap.size()));
  }

  public static void clean(CollectType collectType, int platType) {
	List<String> pmids = queryFromMysql(2);
	System.out.println(String.format("用户收藏%s媒体数量->%d", platType == 2 ? "微博" : platType == 1 ? "微信" : "输入平台值不合法", pmids.size()));
	Map<String, Object> pmidMap = queryFromMongo(collectType);
	System.out.println(String.format("调度->%s数量->%d", collectType.name(), pmidMap.size()));
	pmids.forEach(pmid -> pmidMap.remove(pmid));
	DeleteResult deleteResult = WxArticleReload.MongoUtil.INSTACENEWMONGO.getDatabase("uranus").getCollection("schedule").deleteMany(Filters.in("_id", pmidMap.values()));
	System.out.println(String.format("删除调度%s数量->%d", collectType.name(), deleteResult.getDeletedCount()));
  }

  public static void main(String[] args) {
	if (args[0].equals("clean")) clean(CollectType.C_WB_A_N, 2);
	else query(CollectType.C_WB_A_N, 2);
  }
}
