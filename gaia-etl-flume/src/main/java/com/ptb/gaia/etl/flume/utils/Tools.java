package com.ptb.gaia.etl.flume.utils;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import org.apache.commons.configuration.ConfigurationException;
import org.bson.Document;
import org.jsoup.Jsoup;
import sun.misc.BASE64Encoder;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by MengChen on 2016/11/18.
 */


public class Tools {

    /*
    返回字符串的 MD5加密
     */

    public static String md5Password(String password) {

        try {
            // 得到一个信息摘要器
            MessageDigest digest = MessageDigest.getInstance("md5");
            byte[] result = digest.digest(password.getBytes());
            StringBuffer buffer = new StringBuffer();
            // 把没一个byte 做一个与运算 0xff;
            for (byte b : result) {
                // 与运算
                int number = b & 0xff;// 加盐
                String str = Integer.toHexString(number);
                if (str.length() == 1) {
                    buffer.append("0");
                }
                buffer.append(str);
            }

            // 标准的md5加密后的结果
            return buffer.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return "";
        }

    }

    /*
    将字符串 每两个字符 作为一个目录 创建目录结构
     */
    public String convertToDir(String preDir) {

        StringBuffer bf = new StringBuffer();
        int preLength = preDir.length();
        String str1 = "";
        String arr[] = new String[preLength / 2];
        for (int i = 0; i < preLength; i++) {
            str1 += preDir.charAt(i);
            if ((i + 1) % 2 == 0) {
                arr[i / 2] = str1;
                bf.append(arr[i / 2]).append("/");
                str1 = "";
            }
        }
        return bf.toString();
    }

    /*
    判断时间是否合法
     */
    public Boolean isValidDate(String dateStr){
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        format.setLenient(false);
        try {
            format.parse(dateStr);
            return true;
        } catch (ParseException e) {
            return false;
        }

    }


    public static void main(String[] args) throws UnsupportedEncodingException, NoSuchAlgorithmException, ConfigurationException, ParseException {

//        long time = 20161215L;
//
//        System.out.println(new SimpleDateFormat("yyyyMMddHHmmss").parse(time+"000000").getTime());
//        System.out.println(new SimpleDateFormat("yyyyMMddHHmmss").parse(time+"235959").getTime());
//        System.out.println(md5Password(null));
//        System.out.println(md5Password("123123123").substring(md5Password("123123123").length()-10,md5Password("123123123").length()));
//        System.out.println(new Tools().convertToDir("12322332233223322332"));

//        String postTime = new SimpleDateFormat("yyyyMMddHH", Locale.CHINA).format(1476180630000L);
//        System.out.println(postTime.substring(0,8));
//        System.out.println(Integer.valueOf(postTime.substring(8,10)));



//        System.out.println(Jsoup.parseBodyFragment("我爱你中国 1231231232").text() );

    }

}
