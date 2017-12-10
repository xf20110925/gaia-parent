package com.ptb.gaia.emchat.comm;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ClientContext {

	/*
	* configure name
	*/

	public static final String CONFIG_NAME = "ptb.properties";

	/*
	 * Configuration Source Type
	 */
	public static final String INIT_FROM_PROPERTIES = "FILE";
	
	public static final String INIT_FROM_CLASS = "CLASS";
	
	/*
	 * Implementation List
	 */
	public static final String JERSEY_API = "jersey";
	
	public static final String HTTPCLIENT_API = "httpclient";
	
	/*
	 * Properties
	 */
	private static final String API_PROTOCAL_KEY = "gaia.emchat.protocal";
	
	private static final String API_HOST_KEY = "gaia.emchat.host";
	
	private static final String API_ORG_KEY = "gaia.emchat.org";
	
	private static final String API_APP_KEY = "gaia.emchat.app";
	
	private static final String APP_CLIENT_ID_KEY = "gaia.emchat.client.id";
	
	private static final String APP_CLIENT_SECRET_KEY = "gaia.emchat.client.secret";
	
	private static final String APP_IMP_LIB_KEY = "gaia.emchat.imp.lib";
	
	private static final Logger log = LoggerFactory.getLogger(ClientContext.class);
	
	private static ClientContext context;
		
	private Boolean initialized = Boolean.FALSE;
	
	private String protocal;
	
	private String host;
	
	private String org;
	
	private String app;
	
	private String clientId;
	
	private String clientSecret;
	
	private String impLib;
	
	private EasemobRestAPIFactory factory;
	
	private TokenGenerator token; // Wrap the token generator
	
	private ClientContext() {};
	
	public static ClientContext getInstance() {
		if( null == context ) {
			context = new ClientContext();
		}
		
		return context;
	}

	public ClientContext init(String type) {
		if( initialized ) {
			log.warn("Context has been initialized already, skipped!");
			return context;
		}

		if( StringUtils.isBlank(type) ) {
			log.warn("Context initialization type was set to FILE by default.");
			type = INIT_FROM_PROPERTIES;
		}

		if( INIT_FROM_PROPERTIES.equals(type) ) {
			initFromPropertiesFile();
		}
		else if( INIT_FROM_CLASS.equals(type) ){
			initFromStaticClass();
		}
		else {
			log.error(MessageTemplate.print(MessageTemplate.UNKNOW_TYPE_MSG, new String[]{type, "context initialization"}));
			return context; // Context not initialized
		}

		// Initialize the token generator by default
		if( context.initialized ) {
			token = new TokenGenerator(context);
		}

		return context;
	}

	public ClientContext init(String type, String protocal, String host, String org, String app, String clientId, String clientSecret, String impLib) {
		if( initialized ) {
			log.warn("Context has been initialized already, skipped!");
			return context;
		}

		if( StringUtils.isBlank(type) ) {
			log.warn("Context initialization type was set to FILE by default.");
			type = INIT_FROM_PROPERTIES;
		}

		if( INIT_FROM_PROPERTIES.equals(type) ) {
			initFromPropertiesFile(protocal, host, org, app, clientId, clientSecret, impLib);
		}
		else if( INIT_FROM_CLASS.equals(type) ){
			initFromStaticClass();
		}
		else {
			log.error(MessageTemplate.print(MessageTemplate.UNKNOW_TYPE_MSG, new String[]{type, "context initialization"}));
			return context; // Context not initialized
		}

		// Initialize the token generator by default
		if( context.initialized ) {
			token = new TokenGenerator(context);
		}

		return context;
	}

	public EasemobRestAPIFactory getAPIFactory() {
		if( !context.isInitialized() ) {
			log.error(MessageTemplate.INVAILID_CONTEXT_MSG);
			throw new RuntimeException(MessageTemplate.INVAILID_CONTEXT_MSG);
		}
		
		if( null == this.factory ) {
			this.factory = EasemobRestAPIFactory.getInstance(context);
		}
		
		return this.factory;
	}
	
	public String getSeriveURL() {
		if (null == context || !context.isInitialized()) {
			log.error(MessageTemplate.INVAILID_CONTEXT_MSG);
			throw new RuntimeException(MessageTemplate.INVAILID_CONTEXT_MSG);
		}

		String serviceURL = context.getProtocal() + "://" + context.getHost() + "/" + context.getOrg() + "/" + context.getApp();

		return serviceURL;
	}
	
	public String getAuthToken() {
		if( null == token ) {
			log.error(MessageTemplate.INVAILID_TOKEN_MSG);
			throw new RuntimeException(MessageTemplate.INVAILID_TOKEN_MSG);
		}
		
		return token.request(Boolean.FALSE);
	}
	
	private void initFromPropertiesFile() {
		PropertiesConfiguration conf = null;
		try {
			conf = new PropertiesConfiguration(CONFIG_NAME);
			if (conf == null){
				log.error(MessageTemplate.print(MessageTemplate.FILE_ACCESS_MSG, new String[]{CONFIG_NAME}));
				return; // Context not initialized
			}
		} catch (ConfigurationException e) {
			e.printStackTrace();
			log.error(MessageTemplate.print(MessageTemplate.FILE_ACCESS_MSG, new String[]{CONFIG_NAME}));
			return; // Context not initialized
		}

		String protocal = conf.getString(API_PROTOCAL_KEY, "https");
		String host = conf.getString(API_HOST_KEY, "a1.easemob.com");
		String org = conf.getString(API_ORG_KEY, "1119160926115559");
		String app = conf.getString(API_APP_KEY, "xiaomitest");
		String clientId = conf.getString(APP_CLIENT_ID_KEY, "YXA67Wt-EI0lEeaNHwlmhQ69aA");
		String clientSecret = conf.getString(APP_CLIENT_SECRET_KEY, "YXA6vcM78L558oXQMH0WQCfCX_9dFps");
		String impLib = conf.getString(APP_IMP_LIB_KEY, "httpclient");


		if( StringUtils.isBlank(protocal) || StringUtils.isBlank(host) || StringUtils.isBlank(org) || StringUtils.isBlank(app) || StringUtils.isBlank(clientId) || StringUtils.isBlank(clientSecret) || StringUtils.isBlank(impLib) ) {
			log.error(MessageTemplate.print(MessageTemplate.INVAILID_PROPERTIES_MSG, new String[]{CONFIG_NAME}));
			return; // Context not initialized
		}
		
		context.protocal = protocal;
		context.host = host;
		context.org = org;
		context.app = app;
		context.clientId = clientId;
		context.clientSecret = clientSecret;
		context.impLib = impLib;
		
		log.debug("protocal: " + context.protocal);
		log.debug("host: " + context.host);
		log.debug("org: " + context.org);
		log.debug("app: " + context.app);
		log.debug("clientId: " + context.clientId);
		log.debug("clientSecret: " + context.clientSecret);
		log.debug("impLib: " + context.impLib);
		
		initialized = Boolean.TRUE;
	}


	private void initFromPropertiesFile(String protocal, String host, String org, String app, String clientId, String clientSecret, String impLib) {

		if( StringUtils.isBlank(protocal) || StringUtils.isBlank(host) || StringUtils.isBlank(org) || StringUtils.isBlank(app) || StringUtils.isBlank(clientId) || StringUtils.isBlank(clientSecret) || StringUtils.isBlank(impLib) ) {
			log.error(MessageTemplate.print(MessageTemplate.INVAILID_PROPERTIES_MSG, new String[]{CONFIG_NAME}));
			return; // Context not initialized
		}

		context.protocal = protocal;
		context.host = host;
		context.org = org;
		context.app = app;
		context.clientId = clientId;
		context.clientSecret = clientSecret;
		context.impLib = impLib;

		log.debug("protocal: " + context.protocal);
		log.debug("host: " + context.host);
		log.debug("org: " + context.org);
		log.debug("app: " + context.app);
		log.debug("clientId: " + context.clientId);
		log.debug("clientSecret: " + context.clientSecret);
		log.debug("impLib: " + context.impLib);

		initialized = Boolean.TRUE;
	}
	private ClientContext initFromStaticClass() {
		// TODO
		return null;
	}

	public String getProtocal() {
		return protocal;
	}

	public String getHost() {
		return host;
	}

	public String getOrg() {
		return org;
	}

	public String getApp() {
		return app;
	}

	public String getClientId() {
		return clientId;
	}

	public String getClientSecret() {
		return clientSecret;
	}
	
	public Boolean isInitialized() {
		return initialized;
	}
	
	public String getImpLib() {
		return impLib;
	}
	
	public static void main(String[] args) {
		ClientContext.getInstance().init(null);
	}
	
}
