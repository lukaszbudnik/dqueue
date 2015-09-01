package com.github.lukaszbudnik.dqueue;


import com.codahale.metrics.MetricRegistry;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;

import javax.inject.Named;

public class QueueClientBuilder {

    private static final int DEFAULT_CASSANDRA_PORT = 9042;

    @Inject(optional = true)
    @Named("dqueue.cassandraPort")
    private int cassandraPort = DEFAULT_CASSANDRA_PORT;

    @Inject
    @Named("dqueue.cassandraAddress")
    private String cassandraAddress;

    @Inject
    @Named("dqueue.cassandraKeyspace")
    private String cassandraKeyspace;

    @Inject
    @Named("dqueue.cassandraTablePrefix")
    private String cassandraTablePrefix;

    @Inject(optional = true)
    @Named("dqueue.cassandraCreateTables")
    private boolean cassandraCreateTables = true;

    private CuratorFramework zookeeperClient;

    private MetricRegistry metricRegistry;

    public QueueClientBuilder(int cassandraPort, String cassandraAddress, String cassandraKeyspace, String cassandraTablePrefix, boolean cassandraCreateTables, CuratorFramework zookeeperClient, MetricRegistry metricRegistry) {
        this.cassandraPort = cassandraPort;
        this.cassandraAddress = cassandraAddress;
        this.cassandraKeyspace = cassandraKeyspace;
        this.cassandraTablePrefix = cassandraTablePrefix;
        this.cassandraCreateTables = cassandraCreateTables;
        this.zookeeperClient = zookeeperClient;
        this.metricRegistry = metricRegistry;
    }

    public QueueClientBuilder() {
    }

    public QueueClientBuilder withCassandraPort(int cassandraPort) {
        this.cassandraPort = cassandraPort;
        return this;
    }

    public QueueClientBuilder withCassandraAddress(String cassandraAddress) {
        this.cassandraAddress = cassandraAddress;
        return this;
    }

    public QueueClientBuilder withCassandraKeyspace(String cassandraKeyspace) {
        this.cassandraKeyspace = cassandraKeyspace;
        return this;
    }

    public QueueClientBuilder withCassandraTablePrefix(String cassandraTablePrefix) {
        this.cassandraTablePrefix = cassandraTablePrefix;
        return this;
    }

    public QueueClientBuilder withCassandraCreateTables(boolean cassandraCreateTables) {
        this.cassandraCreateTables = cassandraCreateTables;
        return this;
    }

    public QueueClientBuilder withZookeeperClient(CuratorFramework zookeeperClient) {
        this.zookeeperClient = zookeeperClient;
        return this;
    }

    public QueueClientBuilder withMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        return this;
    }

    public QueueClient build() throws Exception {
        QueueClient queueClient = new QueueClient(cassandraPort, cassandraAddress.split(","), cassandraKeyspace, cassandraTablePrefix, cassandraCreateTables, zookeeperClient, metricRegistry);
        return queueClient;
    }

    public int getCassandraPort() {
        return cassandraPort;
    }

    public void setCassandraPort(int cassandraPort) {
        this.cassandraPort = cassandraPort;
    }

    public String getCassandraAddress() {
        return cassandraAddress;
    }

    public void setCassandraAddress(String cassandraAddress) {
        this.cassandraAddress = cassandraAddress;
    }

    public String getCassandraKeyspace() {
        return cassandraKeyspace;
    }

    public void setCassandraKeyspace(String cassandraKeyspace) {
        this.cassandraKeyspace = cassandraKeyspace;
    }

    public String getCassandraTablePrefix() {
        return cassandraTablePrefix;
    }

    public void setCassandraTablePrefix(String cassandraTablePrefix) {
        this.cassandraTablePrefix = cassandraTablePrefix;
    }

    public boolean isCassandraCreateTables() {
        return cassandraCreateTables;
    }

    public void setCassandraCreateTables(boolean cassandraCreateTables) {
        this.cassandraCreateTables = cassandraCreateTables;
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    public void setMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    public CuratorFramework getZookeeperClient() {
        return zookeeperClient;
    }

    public void setZookeeperClient(CuratorFramework zookeeperClient) {
        this.zookeeperClient = zookeeperClient;
    }
}
