package com.ptb.gaia.etl.flume.utils;

import com.ptb.gaia.service.entity.article.GWxArticleBasic;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.net.ftp.FTPClient;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import static com.ptb.gaia.etl.flume.utils.Tools.md5Password;

/**
 * Created by MengChen on 2016/11/18.
 */
public class FileUtils {

    final static org.slf4j.Logger log = LoggerFactory.getLogger("analysis.ftp");
    static FTPClientConfigure ftpClientConfigure;
    static String wxFtpPath = "";
    private static String lock = "xxxxx";
    static Map<String,FTPClient> ftpMap;

    static{
        try {
            ftpClientConfigure = new FTPClientConfigure();
            wxFtpPath = ftpClientConfigure.getRelativedir();
            ftpMap = new HashedMap();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

        /*
        flume 之前的老方案
         */
    public static boolean uploadFileToFTPWithDir(GWxArticleBasic wxArticleBasic,String filename, String inputStream,String time,int hour){
        FTPClient ftpClient = new FTPClient();
        ftpClient.setConnectTimeout(ftpClientConfigure.getClientTimeout());
        try {
            ftpClient.connect(ftpClientConfigure.getHost(), ftpClientConfigure.getPort());
            ftpClient.login(ftpClientConfigure.getUsername(), ftpClientConfigure.getPassword());
            ftpClient.setFileType(ftpClientConfigure.getTransferFileType());
            ftpClient.setControlEncoding(ftpClientConfigure.getEncoding());
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean ret = false;
        ByteArrayInputStream is = new ByteArrayInputStream(inputStream.getBytes());
        String resultDir = "";
        try {
            String md5Dir = new StringBuffer().append(time).append("/").append(hour).append("/").toString();
            resultDir = wxFtpPath.concat(md5Dir);
            wxArticleBasic.setFilePath(String.valueOf(resultDir.charAt(0)).equals(".") ? resultDir.substring(1,resultDir.length()).concat(filename): resultDir.concat(filename));
            if(!ftpClient.makeDirectory(resultDir)){
                mkDirMany(md5Dir,ftpClient);
            }
            if (!ftpClient.changeWorkingDirectory(resultDir)) {
                log.error("change dir to " + resultDir + " fail");
            }
            ret = ftpClient.storeFile(filename, is);
        } catch (Exception e) {
            log.error("save error " + resultDir + " fail");
        }finally {
            try {
                ftpClient.disconnect();
            } catch (IOException e) {
                log.error(String.format("ftpClient.disconnect %s", e.getMessage()));
            }
        }
        return ret;
    }


      /*
      flume 新方案
       */
    public static void uploadFileToFTPWithDirNew(List<GWxArticleBasic> list,String time,int hour){
        FTPClient ftpClient = new FTPClient();
        String resultDir = "";
        ftpClient.setConnectTimeout(ftpClientConfigure.getClientTimeout());
        try {
            ftpClient.connect(ftpClientConfigure.getHost(), ftpClientConfigure.getPort());
            ftpClient.login(ftpClientConfigure.getUsername(), ftpClientConfigure.getPassword());
            ftpClient.setFileType(ftpClientConfigure.getTransferFileType());
            ftpClient.setControlEncoding(ftpClientConfigure.getEncoding());
            log.info("!!!config!!! : " +ftpClientConfigure.getHost() + "  "+ftpClientConfigure.getPort() +"  " +ftpClientConfigure.getUsername() +"  " + ftpClientConfigure.getPassword());

            // 20161124/12/
            String md5Dir = new StringBuffer().append(time).append("/").append(hour).append("/").toString();
            resultDir = wxFtpPath.concat(md5Dir);
            if (ftpClientConfigure.getPassiveMode().equals("true")) {
                ftpClient.enterLocalPassiveMode();
            }
            if(!ftpClient.changeWorkingDirectory(resultDir)){
                mkDirMany(resultDir,ftpClient);
            }
            ftpClient.changeWorkingDirectory(resultDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for(GWxArticleBasic gWxArticleBasic : list){
            ByteArrayInputStream is = new ByteArrayInputStream(gWxArticleBasic.getContent().getBytes());
            try {
                if(!ftpClient.isAvailable()){ //断开重连
                    ftpClient.connect(ftpClientConfigure.getHost(), ftpClientConfigure.getPort());
                    ftpClient.login(ftpClientConfigure.getUsername(), ftpClientConfigure.getPassword());
                }
               ftpClient.storeFile(md5Password(gWxArticleBasic.getArticleUrl()), is);
            } catch (Exception e) {
                log.error("save error " + gWxArticleBasic.getArticleUrl() + " fail");
                log.info("config : " +ftpClientConfigure.getHost() + "  "+ftpClientConfigure.getPort() +"  " +ftpClientConfigure.getUsername() +"  " + ftpClientConfigure.getPassword());
            }finally {
                try {
                    is.close();
                } catch (IOException e) {
                    log.error(String.format("ftpClient.disconnect %s", e.getMessage()));
                }
            }
        }

        log.info("FTP Success : Size " + list.size());
        if(CollectionUtils.isNotEmpty(list)){
            log.info("FTP One Pmid : " + list.get(0).getArticleUrl() +" Path : " + list.get(0).getFilePath());
        }

        try {
            ftpClient.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static boolean uploadFileToFTPWithDirBatchNew(List<Map<String,String>> list) {
        boolean ret=false;
        FTPClient ftpClient = new FTPClient();
        String resultDir = "";
        ftpClient.setConnectTimeout(ftpClientConfigure.getClientTimeout());
        try {
            ftpClient.connect(ftpClientConfigure.getHost(), ftpClientConfigure.getPort());
            ftpClient.login(ftpClientConfigure.getUsername(), ftpClientConfigure.getPassword());
            ftpClient.setFileType(ftpClientConfigure.getTransferFileType());
            ftpClient.setControlEncoding(ftpClientConfigure.getEncoding());

            // 20161124/12/
            resultDir = list.get(0).get("resultDir");
            if (ftpClientConfigure.getPassiveMode().equals("true")) {
                ftpClient.enterLocalPassiveMode();
            }
            if(!ftpClient.changeWorkingDirectory(resultDir)){
                mkDirMany(resultDir,ftpClient);
            }
            ftpClient.changeWorkingDirectory(resultDir);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for(Map<String,String> b : list){
            ret = false;
            ByteArrayInputStream is = new ByteArrayInputStream(b.get("content").getBytes());
            try {
                if(!ftpClient.isAvailable()){ //断开重连
                    ftpClient.connect(ftpClientConfigure.getHost(), ftpClientConfigure.getPort());
                    ftpClient.login(ftpClientConfigure.getUsername(), ftpClientConfigure.getPassword());
                }
                ret = ftpClient.storeFile(b.get("filename"), is);
            } catch (Exception e) {
                log.info("config : " +ftpClientConfigure.getHost() + "  "+ftpClientConfigure.getPort() +"  " +ftpClientConfigure.getUsername() +"  " + ftpClientConfigure.getPassword());
            }finally {
                try {
                    if(is!=null){
                        is.close();
                    }
                } catch (IOException e) {
                    log.error(String.format("ftpClient.disconnect %s", e.getMessage()));
                }
            }
        }

        try {
            ftpClient.logout();
            ftpClient.disconnect();
            ftpClient=null;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ret;

    }


        /**
         * Description: 将微信正文写入ftp  修复现有数据
         * All Rights Reserved.
         * @param
         * @return
         * @version 1.0  2016-11-23 13:41 by wgh（guanhua.wang@pintuibao.cn）创建
         */
    public static boolean uploadFileToFTPWithDirBatch(List<Map<String,String>> list){
        ByteArrayInputStream is;
        String resultDir = "";
        String key="";
        String fileName = "";
        String content = "";
        String[] c = null;
        boolean ret=false;
        boolean returnRet=false;
        FTPClient ftpClient = null;

        for(Map<String,String> b : list){
            resultDir = b.get("resultDir"); //文件路径
            content = b.get("content"); //正文
            key = b.get("key"); //ftpclient的key
            fileName = b.get("filename"); //文件名
            ret = false;
            is = new ByteArrayInputStream(content.getBytes());
            if(ftpMap.get(key)==null){
                        ftpClient = new FTPClient();
                        try {
                            ftpClient.setConnectTimeout(ftpClientConfigure.getClientTimeout());
                            try {
                                ftpClient.connect(ftpClientConfigure.getHost(), ftpClientConfigure.getPort());
                                ftpClient.login(ftpClientConfigure.getUsername(), ftpClientConfigure.getPassword());
                                ftpClient.setFileType(ftpClientConfigure.getTransferFileType());
                                ftpClient.setControlEncoding(ftpClientConfigure.getEncoding());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
//                            if (ftpClientConfigure.getPassiveMode().equals("true")) {
//                                ftpClient.enterLocalPassiveMode();
//                            }
                            if (!ftpClient.changeWorkingDirectory(resultDir)) { //切换目录失败说明没有此目录
                                    mkDirMany(resultDir, ftpClient); //递归创建远程目录
                            }
                            ftpClient.changeWorkingDirectory(resultDir); //切换目录
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        ftpMap.put(key, ftpClient); //放入池子中等待复用
            }else{
                ftpClient = ftpMap.get(key);
            }
            try {
                if(!ftpClient.isAvailable()){ //断开重连
                    ftpClient.connect(ftpClientConfigure.getHost(), ftpClientConfigure.getPort());
                    ftpClient.login(ftpClientConfigure.getUsername(), ftpClientConfigure.getPassword());
                }
                ret = ftpClient.storeFile(fileName, is);
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } catch (IOException | NullPointerException e ) {
                e.printStackTrace();
                System.out.println("上传文件：" + resultDir + fileName + "失败");
            }finally {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            ftpMap.remove(key);
            ftpClient.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }


        return ret;
    }

    /**
     * Description: 递归创建文件目录
     * All Rights Reserved.
     * @param
     * @return
     * @version 1.0  2016-11-23 13:28 by wgh（guanhua.wang@pintuibao.cn）创建
     */
    public static void mkDirMany(String pathName,FTPClient ftpClient) {
        StringTokenizer s = new StringTokenizer(pathName, "/");
        s.countTokens();
        String tempPath = "";
        while (s.hasMoreElements()) {
            tempPath = tempPath + "/" + (String) s.nextElement();
            try {
                if (!ftpClient.changeWorkingDirectory(tempPath)) { //切换目录失败说明没有此目录
                    ftpClient.mkd(tempPath);
                }
            } catch (Exception e) {
                log.error("ftp文件夹创建失败");
            }

        }
    }



    public static void main(String[] args) {

//        String json = "{\"ch\":[{\"names\":\"怡美家园\",\"data\":[2,2,1,1,1,1],\"times\":[10,11,13,13,21,23]},{\"names\":\"怡美家园\",\"data\":[2,2,1,1,1,1],\"times\":[10,11,13,13,21,23]}]}";
//        JSON.parse(json);
//        System.out.println(JSON.parse(json));

        GWxArticleBasic wxArticleBasic = new GWxArticleBasic();
        GWxArticleBasic wxArticleBasic1 = new GWxArticleBasic();
        List<GWxArticleBasic> list = new ArrayList<>();

        wxArticleBasic.setContent("123");
        wxArticleBasic.setArticleUrl("sadasdasd");
        wxArticleBasic.setFilePath("123");

        wxArticleBasic1.setContent("2234234234");
        wxArticleBasic1.setArticleUrl("dsfsdfsdfsdfdsfsdfdsf");
        wxArticleBasic.setFilePath("123123123");

        list.add(wxArticleBasic);
        list.add(wxArticleBasic1);

        uploadFileToFTPWithDirNew(list,"20161124",12);




    }


}
