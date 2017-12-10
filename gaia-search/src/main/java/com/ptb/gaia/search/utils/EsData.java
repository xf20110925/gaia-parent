package com.ptb.gaia.search.utils;

/**
 * Created by watson zhang on 16/6/30.
 */
public class EsData {
    public String[] hostNames;
    public String clusterName;
    public String indexName;
    public String indexType;
    public long TTl;
    public int batchSize;

    public String[] getHostNames() {
        return hostNames;
    }

    public void setHostNames(String[] hostNames) {
        this.hostNames = hostNames;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public long getTTl() {
        return TTl;
    }

    public void setTTl(long TTl) {
        this.TTl = TTl;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
