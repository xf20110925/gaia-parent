package com.ptb.gaia.emchat.api.impl;


import com.ptb.gaia.emchat.api.ChatMessageAPI;
import com.ptb.gaia.emchat.api.EasemobRestAPI;
import com.ptb.gaia.emchat.comm.constant.HTTPMethod;
import com.ptb.gaia.emchat.comm.helper.HeaderHelper;
import com.ptb.gaia.emchat.comm.wrapper.HeaderWrapper;
import com.ptb.gaia.emchat.comm.wrapper.QueryWrapper;

public class EasemobChatMessage extends EasemobRestAPI implements ChatMessageAPI {

    private static final String ROOT_URI = "/chatmessages";

    public Object exportChatMessages(Long limit, String cursor, String query) {
        String url = getContext().getSeriveURL() + getResourceRootURI();
        HeaderWrapper header = HeaderHelper.getDefaultHeaderWithToken();
        QueryWrapper queryWrapper = QueryWrapper.newInstance().addLimit(limit).addCursor(cursor).addQueryLang(query);
        url = url+"?"+queryWrapper.toUri();

        return getInvoker().sendRequest(HTTPMethod.METHOD_GET, url, header, null, queryWrapper);
    }

    @Override
    public String getResourceRootURI() {
        return ROOT_URI;
    }
}
