package com.ptb.gaia.tool.esTool.util;

import com.ptb.gaia.tool.esTool.search.SearchArticle;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * Created by watson zhang on 2017/1/10.
 */
public class ESClientUtils {
    private static Logger logger = LoggerFactory.getLogger(SearchArticle.class);
    private InetSocketTransportAddress[] serverAddresses;
    private int DEFAULT_PORT = 9300;
    private Client client;

    private void configureHostnames(String[] hostNames) {
        logger.warn(Arrays.toString(hostNames));
        serverAddresses = new InetSocketTransportAddress[hostNames.length];
        for (int i = 0; i < hostNames.length; i++) {
            String[] hostPort = hostNames[i].trim().split(":");
            String host = hostPort[0].trim();
            int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim())
                    : DEFAULT_PORT;
            serverAddresses[i] = new InetSocketTransportAddress(new InetSocketAddress(host, port));
        }
    }

    private void openClient(String clusterName) {
        logger.info("Using ElasticSearch hostnames: {} ",
                Arrays.toString(serverAddresses));
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clusterName)
                .build();

        TransportClient transportClient = TransportClient.builder().settings(settings).build();
        for (InetSocketTransportAddress host : serverAddresses) {
            transportClient.addTransportAddress(host);
        }
        if (client != null) {
            client.close();
        }
        client = transportClient;
    }

}
