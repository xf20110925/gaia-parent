package com.ptb.gaia.tool.utils;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Created by MengChen on 2016/11/18.
 */
public class FileUtils {

    final static org.slf4j.Logger log = LoggerFactory.getLogger("analysis.ftp");
    static FTPClientConfigure ftpClientConfigure;
    static String wxFtpPath = "";
    static Map<String,FTPClient> ftpMap;

    private static FTPClient ftpClient = null;

    static{
        try {
            ftpClientConfigure = new FTPClientConfigure();
            wxFtpPath = ftpClientConfigure.getRelativedir();
            ftpMap = new HashedMap();

            ftpClient = new FTPClient();
            ftpClient.connect(ftpClientConfigure.getHost(), ftpClientConfigure.getPort());
            ftpClient.login(ftpClientConfigure.getUsername(), ftpClientConfigure.getPassword());
            ftpClient.setFileType(ftpClientConfigure.getTransferFileType());
            ftpClient.setControlEncoding(ftpClientConfigure.getEncoding());
            log.info("!!!config!!! : " +ftpClientConfigure.getHost() + "  "+ftpClientConfigure.getPort() +"  " +ftpClientConfigure.getUsername() +"  " + ftpClientConfigure.getPassword());

            // 20161124/12/
            if (ftpClientConfigure.getPassiveMode().equals("true")) {
                ftpClient.enterLocalPassiveMode();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public String getFileByPath(String filePath){
        String resultDir = filePath;
        try {
            InputStream inputStream = ftpClient.retrieveFileStream(resultDir);
            int reply = ftpClient.getReply();
            return inputStream2String(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String inputStream2String (InputStream in) throws IOException {
        StringBuffer out = new StringBuffer();
        byte[] b = new byte[40960];
        for (int n; (n = in.read(b)) != -1;) {
            out.append(new String(b, 0, n));
        }
        return out.toString();
    }

    public static void main(String[] args) {

        FileUtils fileUtils = new FileUtils();
        //uploadFileToFTPWithDirNew(list,"20161124",12);
        String fileByPath;
        fileByPath = fileUtils.getFileByPath("/wxArticle/20160607/12/1e1055895c796c7270e2750e8e251cfd");
        System.out.println(fileByPath);
        fileByPath = fileUtils.getFileByPath("/wxArticle/20160607/12/37b93696f35024932d98081078a50c5b");
        System.out.println(fileByPath);
        fileByPath = fileUtils.getFileByPath("/wxArticle/20160607/12/692cc58d410a3b798a5b26703532a256");
        System.out.println(fileByPath);
        fileByPath = fileUtils.getFileByPath("/wxArticle/20160607/12/939d3c5a7f58437a2d129f8a27b270aa");
        System.out.println(fileByPath);

        return;

    }


}
