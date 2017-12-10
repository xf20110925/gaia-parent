package com.ptb.gaia.emchat.api;

import com.ptb.gaia.emchat.comm.wrapper.BodyWrapper;
import com.ptb.gaia.emchat.comm.wrapper.HeaderWrapper;
import com.ptb.gaia.emchat.comm.wrapper.QueryWrapper;
import com.ptb.gaia.emchat.comm.wrapper.ResponseWrapper;

import java.io.File;

public interface RestAPIInvoker {
	ResponseWrapper sendRequest(String method, String url, HeaderWrapper header, BodyWrapper body, QueryWrapper query);
	ResponseWrapper uploadFile(String url, HeaderWrapper header, File file);
    ResponseWrapper downloadFile(String url, HeaderWrapper header, QueryWrapper query);
}
