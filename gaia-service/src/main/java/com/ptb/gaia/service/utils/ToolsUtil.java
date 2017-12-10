package com.ptb.gaia.service.utils;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by MengChen on 2016/7/25.
 */
public class ToolsUtil implements Serializable {

    final String regEx="[\\_\\〉�\\[\\?《》`~!@#\\$%\\^\\&\\*\\(\\)\\+=|\\{\\}'\\:;',\\[\\].\\<\\>/？~！\\-@#￥%……（）—\\|【】‘；：”“’。，、]";
    public Boolean includeChinaAndEngChar(String str){

        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(str);

        return m.find();

    }

    public static void main(String[] args) {
        System.out.println(new ToolsUtil().includeChinaAndEngChar("我&"));
    }



}
