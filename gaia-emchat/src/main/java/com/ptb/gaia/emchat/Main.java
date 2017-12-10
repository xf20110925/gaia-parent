package com.ptb.gaia.emchat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ptb.gaia.emchat.api.*;
import com.ptb.gaia.emchat.comm.ClientContext;
import com.ptb.gaia.emchat.comm.EasemobRestAPIFactory;
import com.ptb.gaia.emchat.comm.body.IMUserBody;
import com.ptb.gaia.emchat.comm.body.IMUsersBody;
import com.ptb.gaia.emchat.comm.body.TextMessageBody;
import com.ptb.gaia.emchat.comm.wrapper.BodyWrapper;
import com.ptb.gaia.emchat.comm.wrapper.ResponseWrapper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class Main {

	public static void main(String[] args) throws Exception {
		EasemobRestAPIFactory factory = ClientContext.getInstance().init(ClientContext.INIT_FROM_PROPERTIES).getAPIFactory();
		
		IMUserAPI user = (IMUserAPI)factory.newInstance(EasemobRestAPIFactory.USER_CLASS);
		ChatMessageAPI chat = (ChatMessageAPI)factory.newInstance(EasemobRestAPIFactory.MESSAGE_CLASS);
		FileAPI file = (FileAPI)factory.newInstance(EasemobRestAPIFactory.FILE_CLASS);
		SendMessageAPI message = (SendMessageAPI)factory.newInstance(EasemobRestAPIFactory.SEND_MESSAGE_CLASS);
		ChatGroupAPI chatgroup = (ChatGroupAPI)factory.newInstance(EasemobRestAPIFactory.CHATGROUP_CLASS);
		ChatRoomAPI chatroom = (ChatRoomAPI)factory.newInstance(EasemobRestAPIFactory.CHATROOM_CLASS);

        ResponseWrapper fileResponse = (ResponseWrapper) file.uploadFile(new File("/Users/watsonzhang/Downloads/srccode/gaia-parent/gaia-emchat/01.jpg"));
        String uuid = ((ObjectNode) fileResponse.getResponseBody()).get("entities").get(0).get("uuid").asText();
        String shareSecret = ((ObjectNode) fileResponse.getResponseBody()).get("entities").get(0).get("share-secret").asText();
        InputStream in = (InputStream) ((ResponseWrapper) file.downloadFile(uuid, shareSecret, false)).getResponseBody();
        FileOutputStream fos = new FileOutputStream("/Users/watsonzhang/Downloads/srccode/gaia-parent/gaia-emchat/01.jpg");
        byte[] buffer = new byte[1024];
        int len1 = 0;
        while ((len1 = in.read(buffer)) != -1) {
            fos.write(buffer, 0, len1);
        }
        fos.close();

        String cursor = "MTQzNjgyOTEyMDpnR2tBQVFNQWdHa0FCZ0ZYdF84WmdBQ0FkUUFRY0ZXbVVwQlZFZWE0aUpzd0R6WDlCQUNBZFFBUWNGV21TSkJWRWVhMWRyZGNvSm9pUGdB";
        Object o = chat.exportChatMessages(100l, cursor, "select+*+where+timestamp>1403164734226");

        System.out.println(o);
        // Create a IM user
/*		BodyWrapper userBody = new IMUserBody("User101", "123456", "HelloWorld");
		ResponseWrapper newIMUserSingle = (ResponseWrapper) user.createNewIMUserSingle(userBody);
		if (newIMUserSingle.getResponseStatus() == 400){
			System.out.println(((ObjectNode)newIMUserSingle.getResponseBody()).size());
		}

		String[] userNames = {"test", "1081"};
		TextMessageBody textMessageBody = new TextMessageBody("users", userNames, "admin", null, "sever test");
        textMessageBody.getBody();
        Object o = message.sendMessage(textMessageBody);


        System.out.println(o);
        // Create some IM users
		List<IMUserBody> users = new ArrayList<IMUserBody>();
		users.add(new IMUserBody("User002", "123456", null));
		users.add(new IMUserBody("User003", "123456", null));
		BodyWrapper usersBody = new IMUsersBody(users);
		user.createNewIMUserBatch(usersBody);
		
		// Get a IM user
		user.getIMUsersByUserName("User001");
		
		// Get a fake user
		user.getIMUsersByUserName("FakeUser001");
		
		// Get 12 users
		user.getIMUsersBatch(null, null);*/
	}

}
