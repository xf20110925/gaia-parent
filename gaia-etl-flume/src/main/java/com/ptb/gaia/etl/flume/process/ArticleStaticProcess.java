package com.ptb.gaia.etl.flume.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ptb.gaia.etl.flume.utils.Constant;
import com.ptb.gaia.etl.flume.utils.FTPClientConfigure;
import com.ptb.gaia.etl.flume.utils.GaiaConvertUtils;
import com.ptb.gaia.service.entity.article.GWbArticleBasic;
import com.ptb.gaia.service.entity.article.GWxArticleBasic;
import com.ptb.utils.log.LogUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;

import static com.ptb.gaia.etl.flume.utils.FileUtils.uploadFileToFTPWithDirNew;
import static com.ptb.gaia.etl.flume.utils.Tools.md5Password;
import static com.ptb.utils.log.LogUtils.ActionResult.failed;

/**
 * Created by xuefeng on 2016/6/23.
 */
public class ArticleStaticProcess extends ProcessBasic {
    static Logger logger = LoggerFactory.getLogger(ArticleStaticProcess.class);
    static FTPClientConfigure ftpClientConfigure;
    static String wxFtpPath = "";


    public ArticleStaticProcess() throws ConfigurationException {
        super();
    }

    static{

        ftpClientConfigure = new FTPClientConfigure();
        wxFtpPath = ftpClientConfigure.getRelativedir();

    }


    @Override
    public void process(List<Event> events) {
        ArrayList<GWxArticleBasic> gWxArticleBasics = new ArrayList<>();
        ArrayList<GWxArticleBasic> wxFtpList = new ArrayList<>();
        ArrayList<GWbArticleBasic> gWbArticleBasics = new ArrayList<>();
        Calendar c = Calendar.getInstance(Locale.CHINA);
        String time = new SimpleDateFormat("yyyyMMdd",Locale.CHINA).format(c.getTimeInMillis());
        int hour = c.get(c.HOUR_OF_DAY);

        events.stream().forEach(event -> {
            try {
                JSONObject jsonObject = (JSONObject) JSON.parse(event.getBody());
                switch (jsonObject.getInteger("plat")) {
                    case Constant.P_WEIXIN:
                        GWxArticleBasic wxArticleBasic = GaiaConvertUtils.convertJSONObjectToGWxArticleBasic(jsonObject);
                        if(StringUtils.isNotEmpty(wxArticleBasic.getArticleUrl()) && StringUtils.isNotEmpty(wxArticleBasic.getContent())){
                            String md5Dir = new StringBuffer().append(time).append("/").append(hour).append("/").toString();
                            String resultDir = wxFtpPath.concat(md5Dir);
                            wxArticleBasic.setFilePath(String.valueOf(resultDir.charAt(0)).equals(".") ? resultDir.substring(1,resultDir.length()).concat(md5Password(wxArticleBasic.getArticleUrl())): resultDir.concat(md5Password(wxArticleBasic.getArticleUrl())));
                            wxFtpList.add(wxArticleBasic);
                        }
                        GWxArticleBasic wxAb = (GWxArticleBasic) wxArticleBasic.clone();
                        String content = Jsoup.parseBodyFragment(wxAb.getContent()).text();
                        wxAb.setContent(content.length()>=200 ?content.substring(0,200): content);
                        gWxArticleBasics.add(wxAb);
                        break;
                    case Constant.P_WEIBO:
                        GWbArticleBasic wbArticleBasic = GaiaConvertUtils.convertJSONObjectToGWbArticleBasic(jsonObject);
                        String wbcontent = Jsoup.parseBodyFragment(wbArticleBasic.getContent()).text();
                        wbArticleBasic.setContent(wbcontent.length()>=200 ?wbcontent.substring(0,200): wbcontent);
                        gWbArticleBasics.add(wbArticleBasic);
                        break;
                    default:
                        break;
                }

            } catch (Exception e) {
                logger.info(String.format("error data formate events >> [%s]", new String(event.getBody())), e);
                LogUtils.log("gaia-flume", "article static data formate", failed, JSON.toJSONString(event) + e);
            }
        });
        if(CollectionUtils.isNotEmpty(wxFtpList)) uploadFileToFTPWithDirNew(wxFtpList,time,hour);
        if (!gWxArticleBasics.isEmpty()) iGaia.addWxArticleBasicBatch(gWxArticleBasics);
        if (!gWbArticleBasics.isEmpty()) iGaia.addWbArticleBasicBatch(gWbArticleBasics);
    }
}
