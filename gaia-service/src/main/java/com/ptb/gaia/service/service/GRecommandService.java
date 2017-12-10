package com.ptb.gaia.service.service;

import com.alibaba.fastjson.JSON;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateManyModel;
import com.ptb.gaia.service.entity.media.RecommendCategoryMedia;

import org.apache.commons.configuration.ConfigurationException;
import org.bson.Document;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by eric on 16/8/3.
 */
public class GRecommandService extends GaiaServiceBasic {

	public GRecommandService() throws ConfigurationException {
	}

	public String getId(RecommendCategoryMedia recommendCategoryMedia) {
		return String.format("%s:::%s",recommendCategoryMedia.getPmid(),recommendCategoryMedia.getCategory());
	}
	public Boolean addBatchCategory(List<RecommendCategoryMedia> recommendCategoryMedias) {
	   MongoCollection<Document> recommandCollect = getRecommandCollect();
		List<UpdateManyModel<Document>> list = recommendCategoryMedias.stream().map(recommendCategoryMedia -> {
			String id = getId(recommendCategoryMedia);
			Document parse = Document.parse(JSON.toJSONString(recommendCategoryMedia)).append("_id",id);
			return new UpdateManyModel<Document>(new Document("_id", id),new Document("$set",parse),updateOptions);
		}).collect(Collectors.toList());
		getRecommandCollectByPrimary().bulkWrite(list);
		return true;
	}

/*	public static void main(String[] args) {
		try {
			GRecommandService gRecommandService = new GRecommandService();
			RecommendCategoryMedia recommendCategoryMedia = new RecommendCategoryMedia();
			recommendCategoryMedia.setPmid("ddd");
			recommendCategoryMedia.setCategory("1111");
			recommendCategoryMedia.setMedianame("2222");
			gRecommandService.addBatchCategory(Arrays.asList(recommendCategoryMedia));
		} catch (ConfigurationException e) {


		}
	}*/

	public void delAllRecoomandCategory() {
		getRecommandCollectByPrimary().deleteMany(new Document());
	}

}
