package com.ptb.gaia.service.service;

import com.alibaba.fastjson.JSON;
import com.mongodb.client.model.UpdateManyModel;
import com.ptb.gaia.service.entity.media.GLiveMediaStatic;
import com.ptb.gaia.service.entity.media.GVedioMediaStatic;
import org.apache.commons.configuration.ConfigurationException;
import org.bson.Document;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by liujianpeng on 2017/2/27 0027.
 */
public class GVedioMediaService extends GaiaServiceBasic {

    public GVedioMediaService() throws ConfigurationException {
        super();
    }

    Document ConvertGvedioMediaToDocument(GVedioMediaStatic gVedioMediaStatic){
        Document document = Document.parse(JSON.toJSONString(gVedioMediaStatic));
        document.put("AddTime", System.currentTimeMillis());
        return document;
    }



    public Boolean insertBatch(List<GVedioMediaStatic> gVedioMediaStatics){
        List<UpdateManyModel<Document>> list = gVedioMediaStatics.stream().map(vedio -> {
            return new UpdateManyModel<Document>(new Document("_id", vedio.getAccountId()), new Document("$set", ConvertGvedioMediaToDocument(vedio)), updateOptions);
        }).collect(Collectors.toList());
        getVedioCollectByPrimary().bulkWrite(list);
        return true;

    }

}
