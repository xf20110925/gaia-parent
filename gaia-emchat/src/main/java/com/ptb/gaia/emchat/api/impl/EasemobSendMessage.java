package com.ptb.gaia.emchat.api.impl;

import com.ptb.gaia.emchat.api.EasemobRestAPI;
import com.ptb.gaia.emchat.api.SendMessageAPI;
import com.ptb.gaia.emchat.comm.constant.HTTPMethod;
import com.ptb.gaia.emchat.comm.helper.HeaderHelper;
import com.ptb.gaia.emchat.comm.wrapper.BodyWrapper;
import com.ptb.gaia.emchat.comm.wrapper.HeaderWrapper;

public class EasemobSendMessage extends EasemobRestAPI implements SendMessageAPI {
    private static final String ROOT_URI = "/messages";

    @Override
    public String getResourceRootURI() {
        return ROOT_URI;
    }

    public Object sendMessage(Object payload) {
        String  url = getContext().getSeriveURL() + getResourceRootURI();
        HeaderWrapper header = HeaderHelper.getDefaultHeaderWithToken();
        BodyWrapper body = (BodyWrapper) payload;

        return getInvoker().sendRequest(HTTPMethod.METHOD_POST, url, header, body, null);
    }
}
