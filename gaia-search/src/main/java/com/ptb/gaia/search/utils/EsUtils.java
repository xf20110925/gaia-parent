package com.ptb.gaia.search.utils;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.transport.RemoteTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Optional;

/**
 * Created by eric on 16/8/8.
 */
public class EsUtils {
	static Logger logger  = LoggerFactory.getLogger(EsUtils.class);
	static Configuration conf ;
	private static final String[] esHostNames;
	public static final String ptbEsClusterName;
	private static InetSocketTransportAddress[] esServerAddresses;

	static {
		try {
			conf = new PropertiesConfiguration("ptb.properties");
			esHostNames = conf.getStringArray("gaia.es.hosts");
			esServerAddresses = parseEsAddressByHostName(esHostNames);
			ptbEsClusterName = conf.getString("gaia.es.clusterName");
		} catch (ConfigurationException e) {
			throw new RuntimeException("找不到ptb.properties");
		}
	}


	public static TransportClient getCli() {
		logger.info("Using ElasticSearch hostnames: {} ",
					Arrays.toString(esServerAddresses));
		Settings settings = Settings.settingsBuilder()
				.put("cluster.name", ptbEsClusterName).build();
		TransportClient transportClient = TransportClient.builder().settings(settings).build();
		for (InetSocketTransportAddress host : esServerAddresses) {
			transportClient.addTransportAddress(host);
		}
		return transportClient;
	}

	private static InetSocketTransportAddress[] parseEsAddressByHostName(String[] hostNames) {
		logger.warn(Arrays.toString(hostNames));
		esServerAddresses = new InetSocketTransportAddress[hostNames.length];
		for (int i = 0; i < hostNames.length; i++) {
			String[] hostPort = hostNames[i].trim().split(":");
			String host = hostPort[0].trim();
			int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim()) : 9300;
			esServerAddresses[i] = new InetSocketTransportAddress(new InetSocketAddress(host, port));
		}
		return esServerAddresses;
	}

	public static Optional<SearchResponse> search(Client client, String indexName, String indexType, QueryBuilder qb, SortBuilder sort, int startNum, int offset) {
		try {
			SearchResponse response = client.prepareSearch(indexName)
					.setTypes(indexType)
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
					.setQuery(qb)
					.addSort(sort)
					.setFrom(startNum)
					.setSize(offset)
					.setTrackScores(true)
					.execute()
					.actionGet();
			return Optional.of(response);
		} catch (SearchParseException e){
			logger.error("{}", e);
			return Optional.empty();
		} catch (RemoteTransportException e){
			logger.error("{}", e);
			return Optional.empty();
		} catch (Exception e){
			logger.error("searchArticle sort failed! startNum: {}, offset: {}, indexType: {}", startNum, offset, indexType);
			logger.error("{}", e);
			return Optional.empty();
		}
	}
}
