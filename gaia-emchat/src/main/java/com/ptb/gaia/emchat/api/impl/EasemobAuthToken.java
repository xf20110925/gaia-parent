package com.ptb.gaia.emchat.api.impl;

import com.ptb.gaia.emchat.api.AuthTokenAPI;
import com.ptb.gaia.emchat.api.EasemobRestAPI;
import com.ptb.gaia.emchat.comm.body.AuthTokenBody;
import com.ptb.gaia.emchat.comm.constant.HTTPMethod;
import com.ptb.gaia.emchat.comm.helper.HeaderHelper;
import com.ptb.gaia.emchat.comm.wrapper.BodyWrapper;
import com.ptb.gaia.emchat.comm.wrapper.HeaderWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EasemobAuthToken extends EasemobRestAPI implements AuthTokenAPI {
	
	public static final String ROOT_URI = "/token";
	
	private static final Logger log = LoggerFactory.getLogger(EasemobAuthToken.class);
	
	@Override
	public String getResourceRootURI() {
		return ROOT_URI;
	}

	public Object getAuthToken(String clientId, String clientSecret) {
		String url = getContext().getSeriveURL() + getResourceRootURI();
		BodyWrapper body = new AuthTokenBody(clientId, clientSecret);
		HeaderWrapper header = HeaderHelper.getDefaultHeader();
		
		return getInvoker().sendRequest(HTTPMethod.METHOD_POST, url, header, body, null);
	}
}
