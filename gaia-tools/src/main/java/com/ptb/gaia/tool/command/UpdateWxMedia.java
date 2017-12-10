package com.ptb.gaia.tool.command;

import com.alibaba.fastjson.JSON;
import com.ptb.gaia.bus.kafka.KafkaBus;
import com.ptb.gaia.etl.flume.entity.BasicMediaStatic;
import com.ptb.gaia.service.IGaia;
import com.ptb.gaia.service.IGaiaImpl;
import com.ptb.gaia.service.entity.article.GArticleSet;
import com.ptb.gaia.service.entity.article.GWxArticle;
import com.ptb.gaia.service.entity.media.GWxMediaBasic;
import com.ptb.uranus.server.send.BusSender;
import com.ptb.uranus.spider.weixin.WeixinSpider;
import com.ptb.uranus.spider.weixin.bean.WxAccount;
import com.ptb.uranus.spider.weixin.parse.WxSogouParser;
import com.ptb.utils.exception.PTBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.DocFlavor;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Created by liujianpeng on 2017/3/30 0030.
 */
public class UpdateWxMedia {
    static Logger logger = LoggerFactory.getLogger(UpdateWxMedia.class);
    static IGaia iGaia;
    static {
        try {
            iGaia = new IGaiaImpl();
        }catch (Exception e){
            throw new PTBException("GAIA配置文件不正常", e);
        }
    }

    public static List<String> readTxtFile(String filePath){
        List<String> mediaList = new ArrayList<>();
        try {
            File file=new File(filePath);
            if (file.isFile() && file.exists()){
                InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
                BufferedReader bufferedReader = new BufferedReader(reader);
                String pmid = null;
                while ((pmid=bufferedReader.readLine())!=null){
                    mediaList.add(pmid);
                }
                reader.close();
            }else {
                logger.error("找不到指定文件");
            }
        }catch (Exception e){
            logger.error("读取文件错误");
            e.printStackTrace();
        }
        return mediaList;
    }

    public static void getWxMediaDetail(String filePath){
        WeixinSpider weixinSpider = new WeixinSpider();
        List<String> articleUrlList= iGaia.getWxArticleByPmid(readTxtFile(filePath));
        logger.info(String.format("update totalNum[%s]",articleUrlList.size()));
            articleUrlList.forEach(url -> {
                System.out.println(String.format("url:[%s]",url));
                try {
                    Thread.sleep(500l);
                    Optional<WxAccount> weixinAccountByArticleUrl = weixinSpider.getWeixinAccountByArticleUrl(url);
                    iGaia.updataWxMediaStatic(Arrays.asList(convertWxMediaBasic(weixinAccountByArticleUrl)));
                } catch (Exception e) {
                    System.out.println(String.format("更新失败[%s]",url));
                }
            });
        System.out.println("微信媒体更新完成");
    }

    public static void updateWxMediaByWxArticleUrl(String filePatch){
        WeixinSpider weixinSpider = new WeixinSpider();
        List<String> articleUrlList=readTxtFile(filePatch);
        articleUrlList.forEach(url ->{
            System.out.println(String.format("article url[%s]:",url));
            try {
                Thread.sleep(500l);
                Optional<WxAccount> weixinAccountByArticleUrl = weixinSpider.getWeixinAccountByArticleUrl(url);
                KafkaBus kafkaBus = new KafkaBus();
                kafkaBus.start(false,3);
                BusSender busSender = new BusSender(kafkaBus);
                WxMedia wxMedia = new WxMedia();
                wxMedia.setPlat(1);
                wxMedia.setMediaName(weixinAccountByArticleUrl.get().getNickName());
                wxMedia.setBrief(weixinAccountByArticleUrl.get().getBrief());
                wxMedia.setBiz(weixinAccountByArticleUrl.get().getBiz());
                wxMedia.setWeixinId(weixinAccountByArticleUrl.get().getId());
                String format = String.format("http://open.weixin.qq.com/qr/code/?username=%s", weixinAccountByArticleUrl.get().getId());
                wxMedia.setHeadImg(format);
                wxMedia.setQrCode(format);
                wxMedia.setOriginal(false);
                busSender.sendPhoneMonitor(wxMedia);
                System.out.println(JSON.toJSONString(wxMedia));
                kafkaBus.showdown();
            } catch (InterruptedException e) {
                System.out.println(String.format("更新失败[%s]",url));
            }
        });
        System.out.println("微信媒体更新完成");
    }


    public static GWxMediaBasic convertWxMediaBasic(Optional<WxAccount> wxAccount){
        String nickName = wxAccount.get().getNickName();
        String headImg = wxAccount.get().getHeadImg();
        String brief = wxAccount.get().getBrief();
        String qcCode = String.format("http://open.weixin.qq.com/qr/code/?username=%s",wxAccount.get().getId());
        String biz = wxAccount.get().getBiz();
        String weixinId = wxAccount.get().getId();
        GWxMediaBasic gWxMediaBasic = new GWxMediaBasic();
        gWxMediaBasic.setHeadImage(qcCode);
        gWxMediaBasic.setMediaName(nickName);
        gWxMediaBasic.setBrief(brief);
        gWxMediaBasic.setQrcode(qcCode);
        gWxMediaBasic.setPmid(biz);
        gWxMediaBasic.setWeixinId(weixinId);
        return gWxMediaBasic;
    }





/*    public static void getWxMediaDetail(String filePath){
        WeixinSpider weixinSpider = new WeixinSpider();
        List<String> articleUrlList= iGaia.getWxArticleByPmid(readTxtFile(filePath));
        logger.info(String.format("update totalNum[%s]",articleUrlList.size()));
        try {
            articleUrlList.forEach(url -> {
                logger.info(String.format("url:[%s]",url));
                Optional<WxAccount> weixinAccountByArticleUrl = weixinSpider.getWeixinAccountByArticleUrl(url);
                String nickName = weixinAccountByArticleUrl.get().getNickName();
                String headImg = weixinAccountByArticleUrl.get().getHeadImg();
                String brief = weixinAccountByArticleUrl.get().getBrief();
                String qcCode = String.format("http://open.weixin.qq.com/qr/code/?username=%s",weixinAccountByArticleUrl.get().getId());
                String biz = weixinAccountByArticleUrl.get().getBiz();
                String weixinId = weixinAccountByArticleUrl.get().getId();
                WxMedia wxMedia = new WxMedia();
                wxMedia.setHeadImage(headImg);
                wxMedia.setMediaName(nickName);
                wxMedia.setBrief(brief);
                wxMedia.setQrcode(qcCode);
                wxMedia.setPmid(biz);
                wxMedia.setWeixinId(weixinId);
                WxSogouParser.method2("E:\\ 微信媒体.txt", JSON.toJSONString(wxMedia));
            });
        }catch (Exception e){
            logger.error("微信媒体更新失败");
        }
        logger.info("微信媒体更新完成");
    }*/








    public static void main(String[] args) {
        updateWxMediaByWxArticleUrl("D:\\wxMediaNew.txt");
    }

     static class WxMedia extends com.ptb.uranus.server.send.entity.media.BasicMediaStatic {
        private String authentication = "";
         private String weixinId;
         private String headImg;
         private String qrCode;
         private String biz;
         private String brief;
         private boolean isOriginal;


         public String getHeadImg() {
             return headImg;
         }

         public void setHeadImg(String headImg) {
             this.headImg = headImg;
         }

         public String getQrCode() {
             return qrCode;
         }

         public void setQrCode(String qrCode) {
             this.qrCode = qrCode;
         }

         public boolean isOriginal() {
             return isOriginal;
         }

         public void setOriginal(boolean original) {
             isOriginal = original;
         }

         public String getAuthentication() {
            return authentication;
        }

        public void setAuthentication(String authentication) {
            this.authentication = authentication;
        }

        public String getBrief() {
            return brief;
        }

        public void setBrief(String brief) {
            this.brief = brief;
        }


         public String getBiz() {
             return biz;
         }

         public void setBiz(String biz) {
             this.biz = biz;
         }

         public String getWeixinId() {
            return weixinId;
        }

        public void setWeixinId(String weixinId) {
            this.weixinId = weixinId;
        }
    }

}
