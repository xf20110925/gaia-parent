package com.ptb.gaia.tool.esTool.util;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * @DESC:
 * @VERSION:
 * @author:xuefeng
 * @Date: 2016/7/20
 * @Time: 12:12
 */
public enum ESClient {
	instance;
	String[] hostNames;
	String clusterName;
	TransportClient client;
	BulkRequestBuilder bulkRequest;

	ESClient() {
		initClient();
		bulkRequest = client.prepareBulk();
	}

	private void initClient() {
		try {
			Configuration conf = new PropertiesConfiguration("ptb.properties");
			hostNames = conf.getStringArray("gaia.es.hosts");
			clusterName = conf.getString("gaia.es.clusterName", "ptbes");
		} catch (ConfigurationException e) {
		}
		Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).build();
		client = TransportClient.builder().settings(settings).build();
		Arrays.stream(hostNames).map(hostName -> {
			String[] split = hostName.split(":");
			try {
				return new InetSocketTransportAddress(InetAddress.getByName(split[0]), Integer.parseInt(split[1]));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			return null;
		}).forEach(address -> client.addTransportAddress(address));
	}

	public long count(String typeName, String typeValue, String... indices) {
		return instance.client.prepareCount(indices).setQuery(termQuery(typeName, typeValue)).execute().actionGet().getCount();
	}


	public void addMsg(XContentBuilder msg, String indexName, String type, String id) {
		bulkRequest.add(client.prepareIndex(indexName, type, id).setSource(msg));
	}

	public synchronized void execute() {
		bulkRequest.execute().actionGet();
		bulkRequest = client.prepareBulk();
	}

	public SearchHits getByMediaId(String index, String type, String fieldName, String feildValue){
		SearchResponse response = client.prepareSearch(index)
				.setTypes(type)
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.matchQuery(fieldName, feildValue))                 // Query
				.setExplain(true)
				.execute()
				.actionGet();
		SearchHits hits = response.getHits();
		return hits;

	}

	public static void main(String[] args) {
		SearchHits hits = instance.getByMediaId("media_a", "weixinMedia", "weixinID", "lingdianys");
		for (SearchHit hit : hits) {
			System.out.println(hit.getSource());
		}

	}

}
