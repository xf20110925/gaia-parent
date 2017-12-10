package com.ptb.gaia.tool.emchat.convertUser;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ptb.gaia.emchat.api.IMUserAPI;
import com.ptb.gaia.emchat.api.SendMessageAPI;
import com.ptb.gaia.emchat.comm.ClientContext;
import com.ptb.gaia.emchat.comm.EasemobRestAPIFactory;
import com.ptb.gaia.emchat.comm.body.IMUserBody;
import com.ptb.gaia.emchat.comm.body.TextMessageBody;
import com.ptb.gaia.emchat.comm.wrapper.BodyWrapper;
import com.ptb.gaia.emchat.comm.wrapper.ResponseWrapper;
import com.ptb.gaia.tool.utils.MysqlUtil;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

/**
 * Created by watson zhang on 16/10/12.
 */
public class EmchatConvertEntry {
    private static Logger logger = LoggerFactory.getLogger(EmchatConvertEntry.class);
    private PropertiesConfiguration conf;
    private String ADMIN;
    private String HELLOWORLD;

    MysqlUtil mysqlUtil;

    public EmchatConvertEntry(){
        mysqlUtil = new MysqlUtil();
        mysqlUtil.connectMySQL();
        try {
            conf = new PropertiesConfiguration();
            conf.setEncoding("UTF-8");
            conf.setFileName("ptb.properties");
            conf.load();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        ADMIN = conf.getString("zeus.server.admin");
        HELLOWORLD = conf.getString("zeus.server.helloworld", "欢迎来到小蜜!");
    }

    public List<Long> getUserIdList(){
        long id = 0;
        boolean sw = true;
        EasemobRestAPIFactory factory = ClientContext.getInstance().init(ClientContext.INIT_FROM_PROPERTIES).getAPIFactory();
        IMUserAPI emUser = (IMUserAPI)factory.newInstance(EasemobRestAPIFactory.USER_CLASS);
        while (sw){
            String sql = String.format("select id from user where id > %d", id);
            List<HashMap<String, String>> result = mysqlUtil.result(sql);
            if (result == null || result.size() <= 0){
                sw = false;
            }
            for (HashMap<String, String> map : result) {

                BodyWrapper userBody = new IMUserBody(map.get("id"), map.get("id"), "");
                ResponseWrapper newIMUserSingle = (ResponseWrapper) emUser.createNewIMUserSingle(userBody);
                if (Long.parseLong(map.get("id")) > id){
                    id = Long.parseLong(map.get("id"));
                }
                String[] userNames = new String[1];
                userNames[0] = String.valueOf(map.get("id"));
                //this.sendHelloWorld(userNames, HELLOWORLD);
                if (newIMUserSingle.getResponseStatus() != 200){
                    logger.error("{},{}", map.get("id"),((ObjectNode) newIMUserSingle.getResponseBody()).get("error_description").toString());
                    continue;
                }
            }
        }
        return null;
    }

    private boolean sendHelloWorld(String[] userNames, String msg){

        EasemobRestAPIFactory factory = ClientContext.getInstance().init(ClientContext.INIT_FROM_PROPERTIES).getAPIFactory();
        SendMessageAPI message = (SendMessageAPI)factory.newInstance(EasemobRestAPIFactory.SEND_MESSAGE_CLASS);

        TextMessageBody textMessageBody = new TextMessageBody("users", userNames, ADMIN, null, msg);
        textMessageBody.getBody();
        Object o = message.sendMessage(textMessageBody);
        ResponseWrapper responseWrapper = (ResponseWrapper) o;
        if (responseWrapper.getResponseStatus() != 200){
            logger.error(responseWrapper.getResponseStatus().toString() + "  " + responseWrapper.getResponseBody().toString());
        }
        return true;
    }

    public static void emchatConvertTask(){

        EmchatConvertEntry emchatConvertEntry = new EmchatConvertEntry();
        emchatConvertEntry.getUserIdList();
    }

    public static void main(String[] args){
        EmchatConvertEntry emchatConvertEntry = new EmchatConvertEntry();
        emchatConvertEntry.getUserIdList();
    }
}
