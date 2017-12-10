package com.ptb.gaia.emchat.comm.body;

import com.fasterxml.jackson.databind.node.ContainerNode;
import com.ptb.gaia.emchat.comm.constant.MsgType;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class CommandMessageBody extends MessageBody {
	private String action;

	public CommandMessageBody(String targetType, String[] targets, String from, Map<String, String> ext, String action) {
		super(targetType, targets, from, ext);
		this.action = action;
	}

	public String getAction() {
		return action;
	}

	public ContainerNode<?> getBody() {
		if(!this.isInit()){
			this.getMsgBody().put("type", MsgType.CMD);
			this.getMsgBody().put("action", action);
			this.setInit(true);
		}

		return this.getMsgBody();
	}

	@Override
	public Boolean validate() {
		return super.validate() && StringUtils.isNotBlank(action);
	}
}
